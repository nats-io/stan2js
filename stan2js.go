package stan2js

import (
	"errors"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/nats-io/jsm.go/natscontext"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	"gopkg.in/yaml.v3"
)

// ReadConfig reads the configuration file and returns config object.
func ReadConfig(path string) (*Config, error) {
	configBytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	var config Config
	if err := yaml.Unmarshal(configBytes, &config); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	return &config, nil
}

type Config struct {
	STAN     string              `yaml:"stan"`
	Cluster  string              `yaml:"cluster"`
	Client   string              `yaml:"client"`
	NATS     string              `yaml:"nats"`
	Channels map[string]*Channel `yaml:"channels"`
	Clients  map[string]*Client  `yaml:"clients"`
}

// Validate validates the configuration and sets default values
func (c *Config) Validate() error {
	if c.STAN == "" {
		return errors.New("stan context is required")
	}

	if c.Cluster == "" {
		return errors.New("stan cluster is required")
	}

	if c.NATS == "" {
		return errors.New("nats context is required")
	}

	if c.Client == "" {
		c.Client = "stan2js"
	}

	streamChannels := make(map[string]string)
	channelStreams := make(map[string]string)

	for k, x := range c.Channels {
		if err := x.validate(); err != nil {
			return fmt.Errorf("channel %s: %w", k, err)
		}

		x.Name = k

		if x.Stream.Context == "" {
			x.Stream.Context = c.NATS
		}

		if x.Stream.Name == "" {
			x.Stream.Name = k
		}

		if o, ok := streamChannels[x.Stream.Name]; ok {
			return fmt.Errorf("channel %s: stream %q already defined for channel %q", k, x.Stream.Name, o)
		}

		streamChannels[x.Stream.Name] = k
		channelStreams[k] = x.Stream.Name
	}

	for k, x := range c.Clients {
		if err := x.validate(); err != nil {
			return fmt.Errorf("client %s: %w", k, err)
		}

		x.ID = k

		if x.Context == "" {
			x.Context = c.STAN
		}

		for n, s := range x.Subscriptions {
			if _, ok := c.Channels[s.Channel]; !ok {
				return fmt.Errorf("client %s: subscription %q: channel %q does not defined", k, n, s.Channel)
			}

			s.Name = n

			if s.Consumer.Context == "" {
				s.Consumer.Context = c.NATS
			}

			if s.Consumer.Name == "" {
				s.Consumer.Name = n
			}

			s.Consumer.stream = channelStreams[s.Channel]
		}

		if x.Context == "" {
			x.Context = c.STAN
		}
	}

	return nil
}

type Channel struct {
	Name   string  `yaml:"-"`
	Stream *Stream `yaml:"stream"`
}

func (c *Channel) validate() error {
	return c.Stream.validate()
}

type Stream struct {
	Name         string        `yaml:"name"`
	Context      string        `yaml:"context"`
	Replicas     int           `yaml:"replicas"`
	MaxAge       time.Duration `yaml:"max_age"`
	MaxBytes     string        `yaml:"max_bytes"`
	MaxMsgs      int64         `yaml:"max_msgs"`
	MaxConsumers int           `yaml:"max_consumers"`

	maxBytes uint64
}

func (s *Stream) validate() error {
	if s.MaxBytes != "" {
		mb, err := humanize.ParseBytes(s.MaxBytes)
		if err != nil {
			return fmt.Errorf("stream max bytes: %w", err)
		}
		s.maxBytes = mb
	}

	// Everything else will be validated by the NATS server.
	return nil
}

type Client struct {
	ID            string                   `yaml:"-"`
	Subscriptions map[string]*Subscription `yaml:"subscriptions"`
	Context       string                   `yaml:"context"`
}

func (c *Client) validate() error {
	for _, s := range c.Subscriptions {
		if err := s.validate(); err != nil {
			return err
		}
	}

	return nil
}

type Subscription struct {
	Name     string    `yaml:"-"`
	Channel  string    `yaml:"channel"`
	Queue    string    `yaml:"queue"`
	Consumer *Consumer `yaml:"consumer"`
}

func (s *Subscription) validate() error {
	if s.Consumer == nil {
		s.Consumer = &Consumer{}
	}

	if err := s.Consumer.validate(); err != nil {
		return err
	}

	if s.Queue == "" && s.Consumer.Queue != "" {
		return errors.New("consumer queue name set, but subscription is not a queue")
	}

	return nil
}

type Consumer struct {
	Name    string `yaml:"name"`
	Queue   string `yaml:"queue"`
	Pull    bool   `yaml:"pull"`
	Context string `yaml:"context"`

	stream string
}

func (c *Consumer) validate() error {
	if c.Queue != "" && c.Pull {
		return errors.New("consumer: both queue and pull cannot be set")
	}

	return nil
}

type Result struct {
	Channels      []*ChannelResult
	Subscriptions []*SubscriptionResult
}

type ChannelResult struct {
	Channel string
	Stream  string
	OldSeq  uint64
	NewSeq  uint64
}

type SubscriptionResult struct {
	Client       string
	Channel      string
	Stream       string
	Subscription string
	Consumer     string
	Queue        string
	Pull         bool
	OldSeq       uint64
	NewSeq       uint64
}

func newStan(cluster, client, context string) (stan.Conn, error) {
	nc, err := natscontext.Connect(context)
	if err != nil {
		return nil, err
	}

	sc, err := stan.Connect(cluster, client, stan.NatsConn(nc))
	if err != nil {
		nc.Close()
		return nil, err
	}

	return sc, nil
}

func lastSubSeq(cluster string, client *Client, sub *Subscription) (uint64, error) {
	sc, err := newStan(cluster, client.ID, client.Context)
	if err != nil {
		return 0, err
	}
	defer sc.NatsConn().Close()
	defer sc.Close()

	ch := sub.Channel
	qn := sub.Queue

	var ss stan.Subscription
	seqch := make(chan uint64, 1)

	// Setup the callback to get the last sequence for the durable.
	cb := func(m *stan.Msg) {
		ss.Close()
		seqch <- m.Sequence
	}

	// Connect using the same durable name, but do not ack to prevent
	// progressing the sub state.
	if qn == "" {
		ss, err = sc.Subscribe(
			ch,
			cb,
			stan.DurableName(sub.Name),
			stan.SetManualAckMode(),
			stan.MaxInflight(1),
		)
	} else {
		ss, err = sc.QueueSubscribe(
			ch,
			qn,
			cb,
			stan.DurableName(sub.Name),
			stan.SetManualAckMode(),
			stan.MaxInflight(1),
		)
	}

	if err != nil {
		return 0, fmt.Errorf("client %s: subscription %s: %w", client.ID, sub.Name, err)
	}

	return <-seqch, nil
}

func migrateChannel(context, cluster, id string, ch *Channel, durSeqMap *subSeqMap) (uint64, uint64, error) {
	sc, err := newStan(cluster, id, context)
	if err != nil {
		return 0, 0, err
	}
	defer sc.NatsConn().Close()
	defer sc.Close()

	nc, err := natscontext.Connect(ch.Stream.Context)
	if err != nil {
		return 0, 0, fmt.Errorf("channel %s: context %q: %w", ch.Name, ch.Stream.Context, err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		return 0, 0, fmt.Errorf("NATS context %q: %w", ch.Stream.Context, err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:         ch.Stream.Name,
		Subjects:     []string{ch.Name},
		Storage:      nats.FileStorage,
		Replicas:     ch.Stream.Replicas,
		MaxMsgs:      ch.Stream.MaxMsgs,
		MaxBytes:     int64(ch.Stream.maxBytes),
		MaxAge:       ch.Stream.MaxAge,
		MaxConsumers: ch.Stream.MaxConsumers,
	})
	if err != nil {
		return 0, 0, fmt.Errorf("channel %s: create stream %q: %w", ch.Name, ch.Stream.Name, err)
	}

	seqch := make(chan uint64, 1)
	done := make(chan error, 1)

	// Create an ephemeral subscription to get the latest sequence
	// in the channel. This will be used to know when the stream
	// migration is done.
	var sub stan.Subscription
	sub, err = sc.Subscribe(ch.Name, func(m *stan.Msg) {
		sub.Unsubscribe()
		seqch <- m.Sequence
	}, stan.StartWithLastReceived())
	if err != nil {
		return 0, 0, fmt.Errorf("channel %s: subscribe: %w", ch.Name, err)
	}

	oldseq := <-seqch

	// Ephemeral subscription to get all messages.
	sub, err = sc.Subscribe(ch.Name, func(m *stan.Msg) {
		// Create a equivalent messgae for JetStream.
		msg := nats.NewMsg(m.Subject)
		msg.Data = m.Data

		// For reference, set the channel sequence and timestamp
		// since these will be new in the stream.
		msg.Header.Set("Nats-Streaming-Sequence", fmt.Sprintf("%d", m.Sequence))
		msg.Header.Set("Nats-Streaming-Timestamp", fmt.Sprintf("%d", m.Timestamp))

		// Publish the message to the stream and receive the
		// publish ack.
		pa, err := js.PublishMsg(msg)
		if err != nil {
			sub.Unsubscribe()
			done <- fmt.Errorf("channel %s: publish to stream: %w", ch.Name, err)
		}

		// For all durables, update the sequence map with the
		// new stream sequence.
		for k, sb := range durSeqMap.m[ch.Name] {
			if sb[0] == m.Sequence {
				durSeqMap.set(ch.Name, k[0], k[1], sb[0], pa.Sequence)
				seqch <- pa.Sequence
			}
		}

		// Once we have reached the last sequence, we are done.
		if m.Sequence == oldseq {
			sub.Unsubscribe()
			seqch <- pa.Sequence
		}
	}, stan.DeliverAllAvailable())
	if err != nil {
		return 0, 0, fmt.Errorf("channel %s: migrate to stream: %w", ch.Name, err)
	}

	var newseq uint64
	select {
	case newseq = <-seqch:
	case err := <-done:
		return 0, 0, err
	}

	return oldseq, newseq, nil
}

func migrateSubscription(cl *Client, sb *Subscription, newseq uint64) error {
	nc, err := natscontext.Connect(sb.Consumer.Context)
	if err != nil {
		return fmt.Errorf("NATS context %q: %w", sb.Consumer.Context, err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("NATS context %q: %w", sb.Consumer.Context, err)
	}

	str := sb.Consumer.stream
	qn := sb.Consumer.Queue

	cc := &nats.ConsumerConfig{
		Name:        sb.Consumer.Name,
		Description: "Migrated from NATS Streaming",
	}
	if qn != "" {
		cc.DeliverGroup = qn
	}

	if newseq > 0 {
		cc.DeliverPolicy = nats.DeliverByStartSequencePolicy
		cc.OptStartSeq = newseq
	} else {
		cc.DeliverPolicy = nats.DeliverAllPolicy
	}

	_, err = js.AddConsumer(str, cc)
	if err != nil {
		return fmt.Errorf("client %s: add consumer %q for subscription %q: %s", cl.ID, sb.Consumer.Name, sb.Name, err)
	}

	return nil
}

type subSeqMap struct {
	// channel -> [client, durable] -> [old seq, new seq]
	m map[string]map[[2]string][2]uint64
}

func (m *subSeqMap) set(ch string, cl, sub string, oseq, nseq uint64) {
	if _, ok := m.m[ch]; !ok {
		m.m[ch] = make(map[[2]string][2]uint64)
	}

	m.m[ch][[2]string{cl, sub}] = [2]uint64{oseq, nseq}
}

func Migrate(config *Config) (*Result, error) {
	durSeqMap := &subSeqMap{
		m: make(map[string]map[[2]string][2]uint64),
	}

	// The first step is to get the last sequence for each durable
	// subscription across clients.
	for _, cl := range config.Clients {
		for _, sb := range cl.Subscriptions {
			lastseq, err := lastSubSeq(config.Cluster, cl, sb)
			if err != nil {
				return nil, fmt.Errorf("lastSubSeq: client %s: sub: %s, %w", cl.ID, sb.Name, err)
			}

			durSeqMap.set(sb.Channel, cl.ID, sb.Name, lastseq, 0)
		}
	}

	streamSeqs := make(map[string][2]uint64)

	// For each channel, create a stream in order to migrate
	// the channel messages.
	for _, ch := range config.Channels {
		oldseq, newseq, err := migrateChannel(
			config.STAN,
			config.Cluster,
			config.Client,
			ch,
			durSeqMap,
		)
		if err != nil {
			return nil, fmt.Errorf("migrateChannel: channel: %s: %w", ch.Name, err)
		}

		streamSeqs[ch.Stream.Name] = [2]uint64{oldseq, newseq}
	}

	// Migrate the durables.
	for _, cl := range config.Clients {
		for _, sb := range cl.Subscriptions {
			err := migrateSubscription(cl, sb, durSeqMap.m[sb.Channel][[2]string{cl.ID, sb.Name}][1])
			if err != nil {
				fmt.Printf("%#v\n", durSeqMap.m)
				return nil, fmt.Errorf("migrateSubscription: client: %s: sub: %s: %w", cl.ID, sb.Name, err)
			}
		}
	}

	var r Result

	for cn, ch := range config.Channels {
		r.Channels = append(r.Channels, &ChannelResult{
			Channel: cn,
			Stream:  ch.Stream.Name,
			OldSeq:  streamSeqs[ch.Stream.Name][0],
			NewSeq:  streamSeqs[ch.Stream.Name][1],
		})
	}

	for cn, client := range config.Clients {
		for sn, sub := range client.Subscriptions {
			key := [2]string{cn, sn}
			seqs := durSeqMap.m[sub.Channel][key]

			r.Subscriptions = append(r.Subscriptions, &SubscriptionResult{
				Client:       cn,
				Channel:      sub.Channel,
				Stream:       sub.Consumer.stream,
				Subscription: sn,
				Consumer:     sub.Consumer.Name,
				Queue:        sub.Consumer.Queue,
				Pull:         sub.Consumer.Pull,
				OldSeq:       seqs[0],
				NewSeq:       seqs[1],
			})
		}
	}

	return &r, nil
}
