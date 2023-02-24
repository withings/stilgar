# Contributing the Stilgar

First thing first: you'll find a design overview at [design.md](design.md).

## Developer environment

You can use Docker Compose to get a working development environment quickly:

    $ docker compose up --build

This setup will include:

- A container which watches for changes and rebuilds Stilgar when you
  change the code
- A beanstalkd server
- A basic HTML page which uses the Rudderstack JS SDK to store a page event.
  Visit it at `localhost:8082`. Feel free to extend it: `test/js`.
- A Clickhouse instance

The development environment uses the configuration file at the root of
the repository (`stilgar.yml`).

## Adding destinations

Implementing a new destination requires 3 steps:

1. Create a module for your destination. It can be a single file, like
   `blackhole.rs`, or a directory with `mod.rs` if you need more
   space.

2. Implement methods from the `Destination` trait to make your
   destination compatible with Stilgar's forwarder.

3. Add a case in `init_destinations` (see `destinations/mod.rs`).

## Rules

There are no rules! But if you write filthy stuff into Stilgar, I (or
my ghost) will come and do just as unspeakable things to you.

For fame and power, you can also add your name and email to
`Cargo.toml`.
