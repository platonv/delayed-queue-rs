# Builds a small image with the dally binary and every backend.
#
#   docker build -t dally .
#   docker run -p 7070:7070 -v dally-data:/data dally

FROM rust:1-bookworm AS build
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p dally \
    && cp target/release/dally /usr/local/bin/dally

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home /data dally \
    && mkdir /data && chown dally /data
COPY --from=build /usr/local/bin/dally /usr/local/bin/dally

USER dally
WORKDIR /data
ENV DALLY_LISTEN=0.0.0.0:7070 \
    DALLY_STORAGE_URL=sqlite:///data/dally.sqlite \
    DALLY_LOG_FORMAT=json
EXPOSE 7070
ENTRYPOINT ["dally"]
CMD ["server"]
