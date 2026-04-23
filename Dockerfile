# Build stage
FROM rust:1.94-slim-bookworm AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy manifests first for dependency caching
COPY Cargo.toml Cargo.lock ./

# Create a dummy main.rs to build dependencies
RUN mkdir -p src && echo "fn main() {}" > src/main.rs
RUN cargo build --release 2>/dev/null || true
RUN rm -rf src

# Copy source code
COPY src ./src

# Build the actual binary (touch main.rs to force rebuild)
RUN touch src/main.rs && cargo build --release

# Runtime stage
FROM debian:bookworm-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Copy the binary from build stage
COPY --from=builder /app/target/release/kyomei-indexer /usr/local/bin/kyomei-indexer

# Default config and ABI mount points
VOLUME ["/app/config", "/app/abis"]

# HTTP API port
EXPOSE 8080

ENTRYPOINT ["kyomei-indexer"]
CMD ["--config", "/app/config/config.yaml"]
