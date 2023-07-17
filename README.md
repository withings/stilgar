# Welcome to Stilgar!

Stilgar is a lightweight, no-fuss, drop-in replacement for Rudderstack.

Key features:

  - Seamlessly compatible with all Rudderstack client SDKs
  - Runs anywhere, with minimal hardware requirements
  - No Kubernetes, Docker or Rudderstack Cloud subscription required
  - No planes (this ain't the Marvel multiverse)
  - Keeps It Simple, Stupid

Supported sources:

  - Any Rudderstack SDK should work, be it web, mobile or whatever

At present, Clickhouse (over gRPC) is the only supported destination.

## Installing

Stilgar can be installed using cargo:

    $ cargo install stilgar

Remember to add `~/.cargo/bin` to your `PATH` as this is the default
install location for cargo.

## Building and running

Build with :

    $ cargo doc --no-deps    # for auto docs
    $ cargo build --release  # actual build
    
Stilgar takes a single runtime argument: the path to its configuration
file. This can also be provided in the `STILGAR_CONFIG` environment
variable. When neither of those is provided, Stilgar tries those
locations in order:

1. /etc/withings/stilgar.yml
2. /etc/withings/stilgar.yaml
3. ~/.config/stilgar/stilgar.yml
4. ~/.config/stilgar/stilgar.yaml
5. ./stilgar.yml
6. ./stilgar.yaml

For configuration instructions, see the
[stilgar.sample.yml](stilgar.sample.yml) file.

## Contributing

If you intend to explore/extend/review the code, start with
[doc/design.md](doc/design.md) for an overview.
