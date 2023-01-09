# syntax=docker/dockerfile:1

FROM rust:1.66-bullseye AS builder

WORKDIR /workspace
COPY . .

RUN cargo build --release

FROM debian:bullseye-slim

COPY --from=builder /workspace/target/release/protohackers /usr/local/bin/protohackers

EXPOSE 8070

ENTRYPOINT ["/usr/local/bin/protohackers"]