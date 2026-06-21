FROM rust:1.95-slim AS chef
RUN apt-get update \
    && apt-get install -y --no-install-recommends libsqlite3-dev pkg-config gcc \
    && rm -rf /var/lib/apt/lists/*
RUN cargo install cargo-chef && rm -rf /usr/local/cargo/registry
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
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