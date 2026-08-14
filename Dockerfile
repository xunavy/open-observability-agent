FROM rust:1.85-bookworm AS builder
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY apps/observability-api ./apps/observability-api
RUN cargo build --release -p observability-api

FROM debian:bookworm-slim
RUN useradd --create-home --uid 10001 app
WORKDIR /app
COPY --from=builder /src/target/release/observability-api /usr/local/bin/observability-api
RUN mkdir -p /app/data && chown -R app:app /app
USER app
ENV OBSERVABILITY_DATA=/app/data/observations.jsonl
EXPOSE 8080
CMD ["/usr/local/bin/observability-api"]
