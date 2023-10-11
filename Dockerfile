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

EXPOSE 5001 8080
VOLUME [ "/var/lib/sqld" ]

RUN groupadd --system --gid 666 sqld
RUN adduser --system --home /var/lib/sqld --uid 666 --gid 666 sqld
WORKDIR /var/lib/sqld
USER sqld

COPY docker-entrypoint.sh /usr/local/bin

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /target/release/sqld /bin/sqld

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["/bin/sqld"]
