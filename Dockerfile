# build libsql-server
FROM rust:slim-bullseye AS chef
RUN apt update \
    && apt install -y libclang-dev clang \
        build-essential tcl protobuf-compiler file \
        libssl-dev pkg-config git\
    && apt clean \
    && cargo install cargo-chef
# We need to install and set as default the toolchain specified in rust-toolchain.toml
# Otherwise cargo-chef will build dependencies using wrong toolchain
# This also prevents planner and builder steps from installing the toolchain over and over again
COPY rust-toolchain.toml rust-toolchain.toml
RUN cat rust-toolchain.toml | grep "channel" | awk '{print $3}' | sed 's/\"//g' > toolchain.txt \
    && rustup update $(cat toolchain.txt) \
    && rustup default $(cat toolchain.txt) \
    && rm toolchain.txt rust-toolchain.toml

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build -p libsql-server --release

# runtime
FROM debian:bullseye-slim
COPY --from=builder /target/release/libsql-server /bin/libsql-server
RUN groupadd --system --gid 666 libsql-server
RUN adduser --system --home /var/lib/libsql-server --uid 666 --gid 666 libsql-server
RUN apt-get update && apt-get install -y ca-certificates
COPY docker-entrypoint.sh /usr/local/bin
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
VOLUME [ "/var/lib/libsql-server" ]
WORKDIR /var/lib/libsql-server
USER libsql-server
EXPOSE 5001 8080
CMD ["/bin/libsql-server"]
