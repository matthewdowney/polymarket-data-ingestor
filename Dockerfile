FROM rust:1.84-slim

WORKDIR /usr/src/app

# Enable multiarch and install build dependencies
RUN dpkg --add-architecture amd64 && \
    apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    gcc \
    gcc-x86-64-linux-gnu \
    libssl-dev:amd64 \
    && rm -rf /var/lib/apt/lists/*

# Add the target
RUN rustup target add x86_64-unknown-linux-gnu

# Set up cross-compilation environment
ENV PKG_CONFIG_ALLOW_CROSS=1
ENV PKG_CONFIG_PATH=/usr/lib/x86_64-linux-gnu/pkgconfig
ENV OPENSSL_DIR=/usr
ENV OPENSSL_INCLUDE_DIR=/usr/include
ENV OPENSSL_LIB_DIR=/usr/lib/x86_64-linux-gnu
ENV CC_x86_64_unknown_linux_gnu=x86_64-linux-gnu-gcc
ENV AR_x86_64_unknown_linux_gnu=x86_64-linux-gnu-ar
ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc

# Copy the source code
COPY src src
COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock

# Build the project
RUN cargo build --release --target x86_64-unknown-linux-gnu

# The binary will be in /usr/src/app/target/x86_64-unknown-linux-gnu/release/feed 