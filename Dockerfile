# Build Stage
FROM rust:1.80-alpine AS builder

RUN apk add --no-cache musl-dev pkgconfig openssl-dev git

WORKDIR /app

# Copy the build context (which includes members and the copied utilx)
COPY . .

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
