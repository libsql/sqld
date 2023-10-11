# TODO: replace athosturso/sqld-builder with something like ghcr.io/libsql/sqld-builder
FROM athosturso/sqld-builder AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM athosturso/sqld-builder AS builder
COPY --from=planner /recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build -p sqld --release

# runtime
FROM debian:bullseye-slim
COPY --from=builder /target/release/sqld /bin/sqld
RUN groupadd --system --gid 666 sqld
RUN adduser --system --home /var/lib/sqld --uid 666 --gid 666 sqld
RUN apt-get update && apt-get install -y ca-certificates
COPY docker-entrypoint.sh /usr/local/bin
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
VOLUME [ "/var/lib/sqld" ]
WORKDIR /var/lib/sqld
USER sqld
EXPOSE 5001 8080
CMD ["/bin/sqld"]
