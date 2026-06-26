# Build Stage
FROM rust:alpine AS builder

RUN apk add --no-cache musl-dev pkgconfig openssl-dev git

WORKDIR /app

# Copy the build context (which includes members and the copied utilx)
COPY . .

# Fix the Cargo workspace patch path for the copied utilx context and delete Cargo.lock files to force local resolution
RUN sed -i 's|../../utilx/core.utilx.vexarr.com|./utilx/core.utilx.vexarr.com|g' Cargo.toml && \
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
