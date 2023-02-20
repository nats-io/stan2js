# stan2js

A one-time data migration tool for NATS Streaming (STAN) data to NATS JetStream.

This will convert the following:

- NATS Streaming channels to NATS JetStream streams
- NATS Streaming durable subscriptions to NATS JetStream consumers

Check out the [example](https://natsbyexample.com/examples/operations/stan2js/cli) to see the general workflow.

Also if you need a general primer on migrating from NATS Streaming to NATS JetStream, check out the [recorded webinar](https://www.youtube.com/watch?v=yKI9YmLx_8A) and [slides](https://docs.google.com/presentation/d/e/2PACX-1vRJ5-gyW7Wignxt_l6sNrCWDXUevm7fChuJBrWIFWygxMMmEh7a_IjnYpkMuPMah-CZPN0Tk50Vhxti/pub?slide=id.g1fa73f6fc1d_0_206).

## Pre-requisites

The [NATS CLI](https://github.com/nats-io/natscli#installation) is required in order to create [configuration contexts](https://github.com/nats-io/natscli#configuration-contexts). These contexts will be referenced in the stan2js configuration file and used during the migration.

### NATS Contexts

As a quick primer a context can be created with the `nats context save` subcommand and supports the following options depending on the authentication and TLS requirements of the STAN deployment or the JetStream-enabled NATS deployment.

```sh
$ nats context save NAME \
  --server=URL \
  --user=USER \
  --password=PASSWORD \
  --creds=FILE \
  --nkey=FILE \
  --tlscert=FILE \
  --tlskey=FILE \
  --tlsca=FILE
```

## Install

Download the latest version, for your platform, from the [releases page](https://github.com/nats-io/stan2js/releases).

## Usage

*The expected usage of this tool is to do a one-time, full migration of the NATS Streaming data. This does require downtime due to the exclusivity behavior of STAN connections with client IDs. The duration will depend on the amount of data in the channels being migrated. However, the time spent for the migration itself will typically be on the order of seconds even for large channels.*

The first step is to save contexts pointing to the STAN deployment and the NATS deployment. If you intend to migrate channels to streams across multiple accounts, please create a [discussion](https://github.com/nats-io/stan2js/discussions), since the setup is a bit more involved.

The second step is to write the stan2js configuration (see below). Once these two tasks are done, the tool can be invoked as follows.

```sh
$ stan2js config.yaml
```

### Configuration

A YAML-based configuration file is used to declare the set of channels and durable subscriptions (per client ID) to be migrated.

The `channels` section declares channels by key such as `events` that should be migrated. An optional set of stream configuration can be declared, such as an alternative name, and stream limits analogous to those that could be defined for channels.

```yaml
stan: stan              # Default STAN context to use.
cluster: my-cluster     # Name of the STAN cluster as defined in the STAN configuration.
client: stan2js         # Client ID to use for migrating the channels. Defaults to stan2js.
                        # Note, this is separate from the client IDs used in your application.
nats: nats              # Default JetStream-enabled NATS context to use.

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
