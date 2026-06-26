# Build Stage
FROM rust:alpine AS builder

RUN apk add --no-cache musl-dev pkgconfig openssl-dev git

WORKDIR /app

# Copy the build context (which includes members and the copied utilx)
COPY . .

# Rewrite git dependencies to local path dependencies for utilx, and clear Cargo.lock
RUN find . -name "Cargo.toml" -exec sed -i 's|git = "https://github.com/shregar1/core.utilx.vexarr.com"|path = "../utilx/core.utilx.vexarr.com"|g' {} + && \
    find . -name "Cargo.lock" -delete

# Build the release binary from the workspace
RUN cargo build --release --bin fluxx

# Runtime Stage
FROM alpine:3.19

RUN apk add --no-cache ca-certificates libgcc openssl curl

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/target/release/fluxx /app/fluxx-gateway

EXPOSE 7007

# Run the gateway binary
CMD ["/app/fluxx-gateway"]
