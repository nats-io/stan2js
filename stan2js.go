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
	STAN     string             `yaml:"stan"`
	Cluster  string             `yaml:"cluster"`
	NATS     string             `yaml:"nats"`
	Channels map[string]Channel `yaml:"channels"`
	Clients  map[string]Client  `yaml:"clients"`
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

	streamChannels := make(map[string]string)
	channelStreams := make(map[string]string)

	for k, x := range c.Channels {
		if err := x.validate(); err != nil {
			return fmt.Errorf("channel %s: %w", k, err)
		}

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

		if x.Context == "" {
			x.Context = c.STAN
		}

		for n, s := range x.Subscriptions {
			if _, ok := c.Channels[s.Channel]; !ok {
				return fmt.Errorf("client %s: subscription %q: channel %q does not defined", k, n, s.Channel)
			}
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
	Stream Stream `yaml:"stream"`
}

func (c *Channel) validate() error {
	return c.Stream.validate()
}

type Stream struct {
	Name         string        `yaml:"stream"`
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
	Subscriptions map[string]Subscription `yaml:"subscriptions"`
	Context       string                  `yaml:"context"`
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
	Channel  string   `yaml:"channel"`
	Queue    string   `yaml:"queue"`
	Consumer Consumer `yaml:"consumer"`
}

func (s *Subscription) validate() error {
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

func Migrate(config *Config) (*Result, error) {
	// Map of NATS connections by context name. There may be multiple
	// STAN connections that use the same underlying NATS connection.
	natsConns := make(map[string]*nats.Conn)
	stanConns := make(map[string]stan.Conn)

	// Initialize the NATS connection for STAN.
	snc, err := natscontext.Connect(config.STAN)
	if err != nil {
		return nil, fmt.Errorf("NATS context %q: %w", config.STAN, err)
	}
	defer snc.Drain()
	natsConns[config.STAN] = snc

	if config.NATS != config.STAN {
		// Initialize the NATS connection for STAN.
		nc, err := natscontext.Connect(config.NATS)
		if err != nil {
			return nil, fmt.Errorf("NATS context %q: %w", config.NATS, err)
		}
		defer nc.Drain()
		natsConns[config.NATS] = nc
	}

	// Initialize the set of STAN connections for each client.
	for k, c := range config.Clients {
		// Initialize a NATS connection for the context if it does not exist.
		if _, ok := natsConns[c.Context]; !ok {
			nc, err := natscontext.Connect(c.Context)
			if err != nil {
				return nil, fmt.Errorf("client %s: NATS context %q: %w", k, c.Context, err)
			}
			defer nc.Drain()
		}

		nc := natsConns[c.Context]
		sc, err := stan.Connect(config.Cluster, k, stan.NatsConn(nc))
		if err != nil {
			return nil, fmt.Errorf("client %s: connect to STAN: %w", k, err)
		}
		defer sc.Close()

		stanConns[k] = sc
	}

	// Using a one-of client ID "stan2js" for the channel migration.
	sc, err := stan.Connect(config.Cluster, "stan2js", stan.NatsConn(snc))
	if err != nil {
		return nil, fmt.Errorf("connect to STAN: %w", err)
	}
	defer sc.Close()

	// channel -> [client, durable] -> [old seq, new seq]
	durSeqMap := make(map[string]map[[2]string][2]uint64)

	// The first step is to get the last sequence for each durable
	// subscription across clients.
	seqch := make(chan uint64)
	for cn, client := range config.Clients {
		for sn, s := range client.Subscriptions {
			ch := s.Channel
			qn := s.Queue

			// Create the map for the channel if it does not exist.
			if _, ok := durSeqMap[ch]; !ok {
				durSeqMap[ch] = make(map[[2]string][2]uint64)
			}

			// Client ID and durable name pair.
			key := [2]string{cn, sn}

			var sub stan.Subscription

			// Setup the callback to get the last sequence for the durable.
			cb := func(m *stan.Msg) {
				sub.Close()
				seqch <- m.Sequence
			}

			// Connect using the same durable name, but do not ack to prevent
			// progressing the sub state.
			sc := stanConns[cn]
			if qn == "" {
				sub, err = sc.Subscribe(
					ch,
					cb,
					stan.DurableName(sn),
					stan.SetManualAckMode(),
					stan.MaxInflight(1),
				)
			} else {
				sub, err = sc.QueueSubscribe(
					ch,
					qn,
					cb,
					stan.DurableName(sn),
					stan.SetManualAckMode(),
					stan.MaxInflight(1),
				)
			}

			// Get the last sequence and close the sub.
			durSeqMap[ch][key] = [2]uint64{<-seqch, 0}
		}
	}

	done := make(chan error)
	streamSeqs := make(map[string][2]uint64)

	// For each channel, create a stream in order to migrate
	// the channel messages.
	for cn, ch := range config.Channels {
		nc := natsConns[ch.Stream.Context]
		js, err := nc.JetStream()
		if err != nil {
			return nil, fmt.Errorf("NATS context %q: %w", ch.Stream.Context, err)
		}

		_, err = js.AddStream(&nats.StreamConfig{
			Name:         ch.Stream.Name,
			Subjects:     []string{cn},
			Storage:      nats.FileStorage,
			Replicas:     ch.Stream.Replicas,
			MaxMsgs:      ch.Stream.MaxMsgs,
			MaxBytes:     int64(ch.Stream.maxBytes),
			MaxAge:       ch.Stream.MaxAge,
			MaxConsumers: ch.Stream.MaxConsumers,
		})
		if err != nil {
			return nil, fmt.Errorf("channel %s: create stream %q: %w", cn, ch.Stream.Name, err)
		}

		// Create an ephemeral subscription to get the latest sequence
		// in the channel. This will be used to know when the stream
		// migration is done.
		var sub stan.Subscription
		sub, err = sc.Subscribe(cn, func(m *stan.Msg) {
			sub.Unsubscribe()
			seqch <- m.Sequence
		}, stan.StartWithLastReceived())
		if err != nil {
			return nil, fmt.Errorf("channel %s: subscribe: %w", cn, err)
		}

		lastseq := <-seqch

		// Ephemeral subscription to get all messages.
		sub, err = sc.Subscribe(cn, func(m *stan.Msg) {
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
				done <- fmt.Errorf("channel %s: publish to stream: %w", cn, err)
			}

			// For all durables, update the sequence map with the
			// new stream sequence.
			for key, s := range durSeqMap[cn] {
				if s[0] == m.Sequence {
					durSeqMap[cn][key] = [2]uint64{m.Sequence, pa.Sequence}
				}
			}

			// Once we have reached the last sequence, we are done.
			if m.Sequence == lastseq {
				sub.Unsubscribe()
				streamSeqs[ch.Stream.Name] = [2]uint64{lastseq, pa.Sequence}
				done <- nil
			}
		}, stan.DeliverAllAvailable())
		if err != nil {
			return nil, fmt.Errorf("channel %s: migrate to stream: %w", cn, err)
		}

		if err := <-done; err != nil {
			return nil, err
		}
	}

	// Migrate the durables.
	for cn, client := range config.Clients {
		for sn, sub := range client.Subscriptions {
			nc := natsConns[sub.Consumer.Context]
			js, err := nc.JetStream()
			if err != nil {
				return nil, fmt.Errorf("NATS context %q: %w", sub.Consumer.Context, err)
			}

			str := sub.Consumer.stream
			qn := sub.Consumer.Queue

			key := [2]string{cn, sn}

			cc := &nats.ConsumerConfig{
				Name:        sub.Consumer.Name,
				Description: "Migrated from NATS Streaming",
			}
			if qn != "" {
				cc.DeliverGroup = qn
			}

			cc.DeliverPolicy = nats.DeliverByStartSequencePolicy
			cc.OptStartSeq = durSeqMap[sub.Channel][key][1]

			_, err = js.AddConsumer(str, cc)
			if err != nil {
				return nil, fmt.Errorf("client %s: add consumer %q for subscription %q: %s", cn, sub.Consumer.Name, sn, err)
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
			seqs := durSeqMap[sub.Channel][key]
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
