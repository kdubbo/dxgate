FROM rust:1.88-bookworm AS builder

WORKDIR /workspace
COPY . .
RUN rm -f toolchain.toml && cargo build --release --bin dxgate

FROM debian:bookworm-slim

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /workspace/target/release/dxgate /usr/local/bin/dxgate

USER 65532:65532
ENTRYPOINT ["/usr/local/bin/dxgate"]
