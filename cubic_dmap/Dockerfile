FROM hexpm/elixir:1.13.2-erlang-24.2-alpine-3.13.6 AS builder

# Install Hex+Rebar
RUN mix local.hex --force && \
  mix local.rebar --force

# Install git and build dependencies
RUN apk --no-cache add curl gcc git libffi-dev make musl-dev openssl-dev

# Install rust (via rustup): the version in Alpine is too old
RUN curl --proto '=https' --tlsv1.2 -sSf https://static.rust-lang.org/rustup/dist/x86_64-unknown-linux-musl/rustup-init > /tmp/rustup-init && \
 chmod +x /tmp/rustup-init && \
 /tmp/rustup-init -y --default-toolchain 1.59.0 --profile minimal && \
 rm /tmp/rustup-init

ENV MIX_ENV=prod PATH=/root/.cargo/bin:$PATH

WORKDIR /root

COPY mix.exs mix.exs
COPY mix.lock mix.lock

RUN mix do deps.get --only prod, deps.compile

COPY config/config.exs config/

RUN mix deps.compile

COPY lib lib

RUN mix compile

COPY config/runtime.exs config/runtime.exs

RUN mix release

# Second stage: copies the files from the builder stage
FROM alpine:3.13.6

RUN apk add --no-cache libstdc++ libgcc libssl1.1 ncurses-libs bash curl dumb-init

# Create non-root user
RUN addgroup -S dmap && adduser -S -G dmap dmap
USER dmap
WORKDIR /home/dmap

# Set environment
ENV MIX_ENV=prod TERM=xterm LANG=C.UTF-8

COPY --from=builder --chown=dmap:dmap /root/_build/prod/rel /home/dmap/rel

# Ensure SSL support is enabled
RUN /home/dmap/rel/cubic_dmap/bin/cubic_dmap eval ":crypto.supports()"

ENTRYPOINT ["/usr/bin/dumb-init", "--"]

HEALTHCHECK CMD ["/home/dmap/rel/cubic_dmap/bin/cubic_dmap", "rpc", "1 + 1"]
CMD ["/home/dmap/rel/cubic_dmap/bin/cubic_dmap", "start"]
