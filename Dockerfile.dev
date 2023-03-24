FROM ubuntu

RUN apt update && apt -qqy install build-essential curl protobuf-compiler netcat-openbsd
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
RUN ~/.cargo/bin/cargo install cargo-watch

WORKDIR /home/stilgar
CMD bash -c "while ! nc -z clickhouse 9100; do sleep 1; done; ~/.cargo/bin/cargo watch -w src -x 'run --profile docker'"
