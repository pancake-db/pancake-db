FROM ubuntu:20.04
MAINTAINER PancakeDB <inquiries@pancakedb.com>
WORKDIR /workdir

RUN apt-get update && apt-get install -y \
  curl \
  build-essential \
  make \
  gcc
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o install_rust.sh && \
  sh install_rust.sh -y
ENV PATH="/root/.cargo/bin:$PATH"
RUN mkdir /pancake_db_data

COPY Cargo.toml /workdir/
RUN cargo fetch
COPY src /workdir/src
RUN cargo build --release

CMD cargo run --release -- --dir /pancake_db_data
