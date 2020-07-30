FROM rust:1.45.0


RUN apt-get update -y && apt-get install -y \
        libclang1-6.0 \
        libclang-6.0-dev \
        libclang-common-6.0-dev \
        llvm-6.0-dev \
        llvm-6.0-runtime \
        llvm-6.0-tools \
        libclang-6.0-dev \
        libclang1-6.0 \
        clang-3.9 \
        zlib1g-dev \
        libsasl2-dev \
        libsasl2-2


# RUN rustup toolchain install nightly

WORKDIR /app/

RUN cd /app/
