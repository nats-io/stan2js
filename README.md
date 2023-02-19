# stan2js

A one-shot data migration tool for NATS Streaming data to NATS JetStream.

This will convert the following:

- NATS Streaming channels to NATS JetStream streams
- NATS Streaming durable subscriptions to NATS JetStream consumers
  - By default these will be push-based

## Usage

The expected usage of this tool is to do a one-time, full migration of the NATS Streaming state. In practice, this would likely be performed once to validate the new JetStream-based client application(s) work with the migrated data during development and testing and then once for a production switch-over.

The [NATS CLI](https://github.com/nats-io/natscli#installation) is required for the migration, but is also highly recommended as a familiar tool for NATS/JetStream applications.

For this migration tool, we will use it to declare and save [configuration contexts](https://github.com/nats-io/natscli#configuration-contexts) to simplify connecting to the STAN and NATS deployments as well as authenticating across each of the client IDs while the migration is running.

Once the contexts are saved and configuration file written (see below), usage of the tool is simply:

```
$ stan2js config.yaml
```

### NATS Contexts

As a quick primer a context can be created with the `nats context add` subcommand and supports the following options depending on the authentication and TLS requirements.

```sh
$ nats context add NAME \
  --server=URL \
  --user=USER \
  --password=PASSWORD \
  --creds=FILE \
  --nkey=FILE \
  --tlscert=FILE \
  --tlskey=FILE \
  --tlsca=FILE
```

### Configuration

A YAML-based configuration file is used to declare the set of channels and durable subscriptions (per client ID) to be migrated.

The `channels` section declares channels by key such as `events` that should be migrated. An optional set of stream configuration can be declared, such as an alternative name, and stream limits analogous to those that could be defined for channels.

```yaml
stan: stan-context      # Default STAN context to use.
cluster: stan-cluster   # Name of the STAN cluster
nats: nats-context      # Default NATS context to use.

channels:
  # The `events` channel with all the options defined.
  events:
    stream:
      name: EVENTS        # Defaults to channel name
      replicas: 3         # Defaults to 1
      max_msgs: 1000      # Defaults to unlimited
      max_age: 2h         # Defaults to unlimited
      max_bytes: 20MB     # Defaults to unlimited
      max_consumers: 10   # Defaults to unlimited

  # The `orders` channel assuming all the defaults.
  orders: {}
```

The second section declares `clients` (by ID) and the set of durable subscriptions. For example, a client ID named `service-1` is declared with one standard subscription and one queue subscription. Each subscription denotes the channel it is a subscription on and the queue name if a queue subscription. In addition, a `context` name can be provided corresponding to an alternate NATS context to be used when establishing the client connection.

```yaml
clients:
  # The `service-1` client has two subscriptions to be converted to consumers.
  service-1:
    context: stan-service-1
    subscriptions:
      orders-sub:
        channel: orders
        consumer:
          name: orders-consumer  # Defaults to subscription name
          pull: true             # Convert to a pull consumer rather than push

      events-sub:
        queue: events-queue
        channel: events
        consumer:
          queue: other-queue-name # Defaults to queue name.

  service-2:
    # etc...
```
