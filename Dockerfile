FROM gradle:6.5-jdk11

RUN mkdir -p /workspace
WORKDIR "/workspace"

RUN wget https://dl.min.io/server/minio/release/linux-amd64/minio && \
    chmod +x minio

COPY . /workspace

RUN gradle clean build
