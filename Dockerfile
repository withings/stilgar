FROM ubuntu

RUN apt update && apt -qqy install build-essential curl protobuf-compiler
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
RUN ~/.cargo/bin/cargo install cargo-watch

WORKDIR /home/stilgar
CMD ~/.cargo/bin/cargo watch -x 'run --profile docker'
