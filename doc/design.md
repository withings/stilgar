# Stilgar: overview and design

Understanding Stilgar boils down to knowing one thing about it: it is
both asynchronous (in code, and through beanstalkd) and
multi-threaded. This explains most of the design decisions made in the
code base.

## Basics

For any destination, the basic lifecycle of an event is a follows:

1. Events are sent to the Stilgar web service
2. The payloads are queued into beanstalkd
3. A separate forwarder thread pops events from the queue and forwards
   them to your destinations

Events are received in POST on the same endpoints as Rudderstack
`[main.rs, routes.rs]`, that is:

- v1/batch
- v1/alias
- v1/group
- v1/identify
- v1/page
- v1/screen
- v1/track

> Stilgar also exposes an extra GET endpoint at `/sourceConfig`. This
> is used to mock Rudderstack's control plane, as some SDKs (JS) will
> refuse to run if a control plane does not validate the write key.

Note that the forwarder has very little logic: it can split batch
events into individual events, and it can delay processing in case of
destination errors. Other than that, it simply calls functions defined
by each destination `[destinations/mod.rs]`.

## Monitoring

Stilgar exposes 2 endpoints for monitoring:

- A `/ping` route which simply replies with *pong*. This route does
  not require authentication and can be used to determine whether
  Stilgar is running or not (eg. in a healthcheck).
- A `/status` route which provides some basic statistics, mostly about
  the beanstalkd queue. This route supports authentication, using
  admin credentials (not the write keys).

# Global event flow diagram

    Web services logic
    ------------------

            +-------+      +---------+      +------------+
            | event +------> /page   +------> beanstalkd |
            +-------+      | /screen |      |   queue    |
                           |   ...   |      +------+-----+
                           +---------+             |
                            API tasks              |
                                                   |
                                                   | reserve
    Forwarder logic                                | job
    ---------------                                |
                                                   |
                                  +-----------+    |
                                  | forwarder <----+
                                  +-----+-----+
                                        |
                                        |
                                        |
                                        |
           +----------------------------+------------+
           |                                         |
           | event                    error recently |
           | is a                        returned by |
           | batch                     a destination |
           |                                         |
           |                                         |
     +-----v------+     +--------------+     +-------v-----+
     | split into |     |   forward    |     |    apply    |
     | individual +----->      to      <-----+ exponential |
     | events     |     | destinations |     |   backoff   |
     +------------+     +--------------+     +-------------+
                               ^
                               |
                               |
     Destination logic         |
     -----------------         +-------------------+
                               |                   |
                               v                   v
                        +- Blackhole -+  +--- Clickhouse --+
                        |             |  |                 |
                        | +---------+ |  | +-------------+ |
                        | | discard | |  | | write event | |
                        | |  event  | |  | |  to cache   | |
                        | +----+----+ |  | +-------+-----+ |
                        |      |      |  |     ... |       |
                        +------+------+  | +-------v-----+ |
                               |         | | flush cache | |
                               v         | |  to server  | |
                               x         | +-------+-----+ |
                                         |         |       |
                                         +---------+-------+
                                                   |
                                                   v
                                              destination

## Destinations

When an event is sent to a destination **within Stilgar**, it does not
necessarily mean that it's been sent out. It means that the event has
been taken out of the queue and passed over to the destination
logic. That code can then decide to hold onto the event in memory for
a bit, or send it straight away. This is useful to improve
performance, when your destination would rather receive the events in
batch.

### Blackhole

This destination `[destinations/blackhole.rs]` can be used for
testing, or as an example to write an actual destination from. It
drops all events.

### Clickhouse

This destination `[destinations/clickhouse]` can send events over to
Yandex Clickhouse, using the gRPC protocol. The logic for this target
is buffered: events will only be sent out once the forwarder has
provided enough of them. Here's the overall flow:

    +-------+      +-------------------+
    | event +------> destination logic |
    +-------+      +---------------+---+
    from the             [mod.rs]  |
    forwarder                      |
                                   |
                    +--------------v--+
                    | in-memory cache |
                    +--------------+--+
                       [cache.rs]  |
                                   |
                                   |
             once enough events    |
             have been received... |
                                   |
                  +----------------v--+
                  | TSV event batches |
                  +-------------------+
                 sent over to Clickhouse
                                  |
                                  v

The in-memory cache stores events grouped by columns. That is, events
with properties a, b, c go one way, those with a, c, d another, and so
on. Once the cache has enough entries, each group is taken separately
and sent in TSV format over to Clickhouse. This means each `INSERT`
query always covers the same set of columns, which allows us to avoid
inefficient input formats like TabSeparatedWithNames or JSONEachRow.

## Forwarder backoff

At times, it might be necessary to slow down or stop events intake as
destination issues arise and resolve. For example, should your
Clickhouse destination become unavailable due to a network issue, it
would be wise to let events accumulate in the beanstalkd queue rather
than the in-memory destination cache.

To this end, every destination known to Stilgar is given a special
*switch* at initialisation through which it can interact with the
forwarder. Sending a message over that channel will notify the
forwarder that something has gone wrong. When that happens, it will
begin applying an exponential backoff on the dequeuing process, until
enough time passes without a notification.

This will progressively slow down the forwarding process and allow
events to stack up in the beanstalkd queue. Configuring your queue to
be persisted on disk will allow you to recover your events even if the
destination issue ends up requiring a Stilgar restart.

## A note about asynchronous Rust

An asynchronous *task* should be treated like a *thread*, even though
the code might be running on a single core. This combined with Rust's
memory safety precautions means special care must be taken when tasks
are sharing data.

While this could probably have been done using reference-counted
pointers like `Arc` and locks, Stilgar goes the other way and uses
*message passing*.

There are a handful of resources which need to be shared in Stilgar:

- The TCP streams to beanstalkd, used by all API routes.
- The TCP streams to Clickhouse
- The Clickhouse in-memory cache
- ...

In Stilgar, each shared resource is *owned* by a task. At a lower
level, this means the resource is available from the task's heap, and
completely inaccessible from other locations. This ensures complete
thread-safety: only one task (running on one thread) can access the
resource, at any given time.

To interact with the shared resource, other tasks use *messages* in
which they describe what they want from the resource. They pass that
message over to the owning task, which processes those sequentially,
in whichever order they arrive. This ensures each operation on the
shared resource is atomic.

Here's the basic flow:

     User task 1              Owner task                   User task 1
    +---------------+        +----------------+           +---------------+
    |               |        |                |           |               |
    |               |        | +------------+ |           |               |
    |               |        | | shared Vec | |           |               |
    |               |        | +------------+ |           |               |
    |               |        |                |           |               |
    | +----------------+     | +------------+ |           |               |
    | | add: 2         <-----+->            | |        +----------------+ |
    | +----------------+     | | process... <-+--------> pop first      | |
    |               |        | |            | |        +----------------+ |
    |               |        | +------------+ |           |               |
    +---------------+        |                |           +---------------+
                             +----------------+

In Rust (with Tokio), those exchanges between tasks take place over
*channels* :

1. A multi-producer, single-consumer channel is created and the
   consumer passed over to the owner task.
2. Each user task has a producer handle. Those can be cloned safely
   and cheaply.
3. The user task creates a oneshot channel and keeps the receiving
   end.
4. The user task creates a message with instructions ("add 2") and the
   transmitting end of the oneshot channel.
5. The owner tasks receives messages sequentially. For each message it
   performs the request operation, and sends a reply across the user's
   oneshot channel.
6. The user task (which was waiting for an answer) receives the
   owner's reply and continues its work.

For these reasons, you will find a handful of tasks, channels and
messages in Stilgar. Here's an example flow for 2 Clickhouse queries,
a `SELECT` and an `UPDATE`, coming from 2 different tasks:

     User task 1              Owner task               User task 2
    +----------------+       +----------------+       +----------------+
    |                |       |                |       |                |
    |                |       | +------------+ |       |                |
    |                |       | | Clickhouse | |       |                |
    |                |       | | connection | |       |                |
    |                | mpsc  | +------------+ |       |                |
    |  +----------------+    |                |       |                |
    |  | SELECT ...     +---->  querying...   |  mpsc |                |
    |  +----------------+    |    .           |    +----------------+  |
    |                |       |    .          .<----+     UPDATE ... |  |
    |                |       |    .          .|    +----------------+  |
    |                |       |    .  task is .|       |                |
    |                |       |    .     busy .|       |                |
    |                |       |    .     with .|       |                |
    |                |       |    .  SELECT, .|       |                |
    |                |       |    .     wait .|       |                |
    |                |       |    .          .|       |                |
    |                |       |    .          .|       |                |
    |                |    +----------------+ .|       |                |
    |                <----+     result set | .|       |                |
    |                |    +----------------+ .|       |                |
    |                |oneshot|               .|       |                |
    |                |       |               .|       |                |
    |                |       | querying... <-'|       |                |
    |                |       |                |       |                |
    |                |       |  +----------------+    |                |
    |                |       |  | UPDATE OK      +---->                |
    |                |       |  +----------------+    |                |
    |                |       |                |oneshot|                |
    +----------------+       +----------------+       +----------------+

Note that waits are not busy waits: task 2 will *yield* and leave the
CPU available for any other task which may need it to run (eg. the
owner task). This works whatever the threading situation is: think
concurrency, not parallelism.

## Contributing

See [contributing.md](contributing.md) next for more details about the
developer setup.
