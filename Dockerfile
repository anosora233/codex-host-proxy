FROM rust:1-bookworm AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
COPY --from=build /build/target/release/codex-host-proxy /usr/local/bin/codex-host-proxy
USER 10001:10001
EXPOSE 8787
ENTRYPOINT ["/usr/local/bin/codex-host-proxy"]
