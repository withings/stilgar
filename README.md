# Welcome to Stilgar!

Stilgar is a lightweight, no-fuss, drop-in replacement for Rudderstack.

Key features:

  - Seamlessly compatible with all Rudderstack client SDKs
  - Runs anywhere, with minimal hardware requirements
  - No Kubernetes, Docker or Rudderstack Cloud subscription required
  - No planes (this ain't the Marvel multiverse)
  - Keeps It Simple, Stupid

Supported sources:

  - Any Rudderstack SDK, be it web, mobile or whatever

Supported destinations:

  - Clickhouse

## Building and running

Build with :

    $ cargo doc --no-deps    # for auto docs
    $ cargo build --release  # actual build
    
Stilgar takes a single runtime argument: the path to its configuration
file. This can also be provided in the `STILGAR_CONFIG` environment
variable. When neither of those is provided, the location is inferred
from the following list:

1. /etc/withings/stilgar.yml
2. /etc/withings/stilgar.yaml
3. ~/.config/stilgar/stilgar.yml
4. ~/.config/stilgar/stilgar.yaml
5. ./stilgar.yml
6. ./stilgar.yaml

## Client examples

The main difference with Rudderstack is that you need to specify both
the control and data plane URLs, and point both to Stilgar. Here's an
example for the JS SDK:

    /* initialise the SDK as usual */
    rudderanalytics.load('your-write-key', 'https://stilgar.example.td/', {
        configUrl: 'https://stilgar.example.td/'
    });
    /* send your events: page, track, screen, ... */

## Contributing

If you intend to explore/extend/review the code, start with
[doc/design.md](doc/design.md) for an overview.
