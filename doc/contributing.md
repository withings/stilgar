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

To run the test container within your development environment, run:

    $ docker compose up --no-deps tester

As a small aside, the crate also comes with an extra binary to
troubleshoot rejected events: stilgar-check-event. To build it, enable
the `checker` feature when building stilgar. You can then pass it a
JSON payload on stdin, and it will try to explain why it is being
rejected.

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

Also: the CI will look for outdated dependencies and fail if anything
isn't up to date. To avoid this issue, install `cargo-outdated` and
configure the following pre-commit hook:

    #!/bin/sh
    which cargo-outdated >/dev/null 2>&1 || exit 0
    echo "pre-commit: looking for outdated dependencies"
    cargo outdated --depth 1 --exit-code 1

When the hook fail, run `cargo update` to update dependencies (and
Cargo.lock). If the update goes against the Cargo.toml requirements,
adjust those first. In any case, make sure you run the tests to make
sure no dependency has gone haywire.

For fame and power, you can also add your name and email to
`Cargo.toml`.
