FROM ubuntu:22.04

RUN apt-get update && \
    apt-get install -y \
    cmake make g++ clang-format \
    python3 python3-pip \
    perf && apt-get clean

RUN pip3 install matplotlib

WORKDIR /workspace
