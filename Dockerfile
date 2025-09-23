# Stage 1: Build the Rust application
FROM rust:1.90 AS builder

# Install protobuf-compiler to provide protoc
RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /usr/src/vecnopool

# Copy Cargo files and build.rs
COPY Cargo.toml Cargo.lock build.rs ./

# Copy .proto files used by tonic-build
COPY proto/ ./proto/

# Create a dummy main.rs to cache dependencies
RUN mkdir src && echo "fn main() {}" > src/main.rs

# Build dependencies to cache them
RUN cargo build --release

# Copy the actual source code
COPY . .

# Build the application
RUN cargo build --release

# Stage 2: Create a minimal runtime image
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libpq-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Copy the compiled binary from the builder stage
COPY --from=builder /usr/src/vecnopool/target/release/vecnopool .

# Copy any additional files needed at runtime (e.g., .env)
COPY .env .env


EXPOSE 6969

# Set environment variables (if needed)
ENV RUST_LOG=info

# Run the application
CMD ["./vecnopool"]