FROM docker.io/rust:1-slim-bullseye AS build
WORKDIR /app

RUN apt update && apt upgrade && apt install -y pkg-config libssl-dev

COPY src /app/src
COPY benches /app/benches
COPY build.rs Cargo.toml Cargo.lock /app/

RUN cargo build --locked --release
RUN cp ./target/release/hematite /bin/server

FROM docker.io/debian:bullseye-slim AS final

RUN apt update && apt upgrade && apt install -y ca-certificates curl && apt-get clean

# See https://docs.docker.com/develop/develop-images/dockerfile_best-practices/
ARG UID=10001
RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    appuser
USER appuser

COPY --from=build /bin/server /bin/

ENV RUST_LOG=info
ENV HEMATITE_STREAMS_DIR="/var/lib/hematite/streams"

EXPOSE 8080
VOLUME ["/var/lib/hematite"]


CMD ["/bin/server"]
