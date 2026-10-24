# Two stages: build with the full Rust toolchain, run on a slim Debian with
# only CA certificates added (the participant, the registry and the token
# endpoint are all reached over TLS).
FROM rust:1.99.1-bookworm AS build
WORKDIR /src
# rust-toolchain.toml asks for "stable"; build with the image's toolchain
# instead of downloading another one.
ENV RUSTUP_TOOLCHAIN=1.99.1
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY src ./src
RUN cargo build --release --locked && strip target/release/canton-settlement-service

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 --home-dir /data settlement \
 && mkdir -p /data && chown settlement /data
COPY --from=build /src/target/release/canton-settlement-service /usr/local/bin/
USER settlement
WORKDIR /data
ENV SETTLEMENT_DB=/data/settlement.db \
    SETTLEMENT_LISTEN=0.0.0.0:8080 \
    SETTLEMENT_LOG_FORMAT=json
EXPOSE 8080
ENTRYPOINT ["canton-settlement-service"]
