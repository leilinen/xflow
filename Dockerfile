FROM rust:1.95-slim AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends libsqlite3-dev pkg-config gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY Cargo.toml Cargo.lock* rust-toolchain.toml ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libsqlite3-0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/xflow /usr/local/bin/xflow
COPY config.docker.yaml ./

EXPOSE 8000
CMD ["xflow", "serve", "--config", "config.docker.yaml"]
