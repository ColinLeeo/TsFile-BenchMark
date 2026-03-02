FROM ubuntu:22.04

RUN apt-get update && apt-get install -y software-properties-common

RUN apt-get update && \
    apt-get install -y wget ca-certificates lsb-release \
    cmake make g++ maven nlohmann-json3-dev \
    python3 python3-pip uuid-dev pkg-config git python3.10-venv \
    linux-tools-common linux-tools-generic \
    openjdk-17-jdk \
    perl && apt-get clean

# Apache Arrow C++ and Parquet (for Parquet C++ benchmark)
RUN wget -q https://packages.apache.org/artifactory/arrow/ubuntu/apache-arrow-apt-source-latest-jammy.deb -O /tmp/arrow-apt.deb && \
    apt-get install -y -V /tmp/arrow-apt.deb && apt-get update && \
    apt-get install -y -V libarrow-dev libparquet-dev && rm -f /tmp/arrow-apt.deb && apt-get clean


RUN wget https://apt.llvm.org/llvm-snapshot.gpg.key
RUN apt-key add llvm-snapshot.gpg.key
RUN add-apt-repository "deb http://apt.llvm.org/jammy/ llvm-toolchain-jammy-17 main" -y 
RUN apt-get install clang-format-17 -y
RUN update-alternatives --install /usr/bin/clang-format clang-format /usr/bin/clang-format-17 100
RUN pip3 install matplotlib tqdm pandas psutil pyarrow

WORKDIR /workspace
