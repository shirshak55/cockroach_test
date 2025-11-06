# Multi-stage build for minimal image size
FROM rust:1.83-slim as builder

WORKDIR /build

# Install required dependencies
RUN apt-get update && \
    apt-get install -y pkg-config libssl-dev && \
    rm -rf /var/lib/apt/lists/*

# Copy manifests
COPY Cargo.toml Cargo.lock ./

# Copy source code
COPY src ./src

# Build for release
RUN cargo build --release

# Runtime stage with minimal image
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && \
    apt-get install -y ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the binary from builder
COPY --from=builder /build/target/release/cockroach_test /app/cockroach_test

# Copy default config (can be overridden with volume mount)
COPY config.toml /app/config.toml

# Create a non-root user
RUN useradd -m -u 1000 benchuser && \
    chown -R benchuser:benchuser /app

USER benchuser

# Run the benchmark
ENTRYPOINT ["/app/cockroach_test"]
