FROM ubuntu:22.04

RUN apt-get update && apt-get install -y software-properties-common

RUN apt-get update && \
    apt-get install -y wget \
    cmake make g++ maven nlohmann-json3-dev \
    python3 python3-pip uuid-dev pkg-config git  python3.10-venv\
    linux-tools-common linux-tools-generic \
    openjdk-17-jdk \    
    perl && apt-get clean


RUN wget https://apt.llvm.org/llvm-snapshot.gpg.key
RUN apt-key add llvm-snapshot.gpg.key
RUN add-apt-repository "deb http://apt.llvm.org/jammy/ llvm-toolchain-jammy-17 main" -y 
RUN apt-get install clang-format-17 -y

RUN pip3 install matplotlib  tqdm pandas psutil

WORKDIR /workspace
