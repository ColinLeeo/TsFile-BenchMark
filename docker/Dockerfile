FROM ubuntu:22.04

RUN apt-get update && \
    apt-get install -y \
    cmake make g++ clang-format maven nlohmann-json3-dev \
    python3 python3-pip uuid-dev pkg-config git  python3.10-venv\
    linux-tools-common linux-tools-generic \
    openjdk-17-jdk \    
    perl && apt-get clean

RUN pip3 install matplotlib  tqdm pandas psutil

WORKDIR /workspace
