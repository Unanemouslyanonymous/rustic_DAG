FROM rust:1-bookworm
WORKDIR /src
COPY . .
RUN cargo build --release --bins && cargo test --release --no-run
CMD ["scripts/demo.sh"]
