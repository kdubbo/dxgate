FROM rust:1.85-bookworm AS builder

WORKDIR /workspace
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/*
COPY . .
RUN rm -f toolchain.toml && cargo build --release --bin dxgate

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /workspace/target/release/dxgate /usr/local/bin/dxgate

USER 65532:65532
ENTRYPOINT ["/usr/local/bin/dxgate"]
