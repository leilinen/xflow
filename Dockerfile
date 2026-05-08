FROM rust:1.95-slim AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends libsqlite3-dev pkg-config gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 1. 先复制依赖描述文件，利用 Docker 层缓存
COPY Cargo.toml Cargo.lock* rust-toolchain.toml ./

# 2. 创建空项目，仅下载和编译依赖（只要 Cargo.toml/Cargo.lock 不变就命中缓存）
RUN mkdir src \
    && echo "" > src/lib.rs \
    && echo "fn main() {}" > src/main.rs \
    && cargo build --release 2>/dev/null || true \
    && rm -rf src

# 3. 复制真实源码，只增量编译项目代码（秒级）
COPY src ./src
RUN touch src/main.rs src/lib.rs && cargo build --release

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libsqlite3-0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/xflow /usr/local/bin/xflow
COPY config.docker.yaml ./

EXPOSE 8000
CMD ["xflow", "serve", "--config", "config.docker.yaml"]
