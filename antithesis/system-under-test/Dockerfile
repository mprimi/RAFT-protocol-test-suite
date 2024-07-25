# Not alpine because docker assumes glibc. alpine uses musl not glibc. Hence why /lib64/ld-linux-x86-64.so.2 doesn't exist.
FROM --platform=x86-64 ubuntu:latest AS builder

# Update and upgrade repo
RUN apt-get update -y -q && apt-get upgrade -y -q

# Install tools we might need
RUN DEBIAN_FRONTEND=noninteractive apt-get install --no-install-recommends -y -q curl build-essential ca-certificates git

# Download Go and install it to /usr/local/go
RUN curl -s https://dl.google.com/go/go1.22.5.linux-amd64.tar.gz | tar -C /usr/local -xz

ENV PATH $PATH:/usr/local/go/bin

# Copy nats-server source code
COPY ./toy-raft /toy-raft

WORKDIR /

# Install instrumentor (lands in /root/go/bin/antithesis-go-instrumentor)
RUN go install github.com/antithesishq/antithesis-sdk-go/tools/antithesis-go-instrumentor@latest

# Instrument code (lands in /nats-server_instrumented)
RUN mkdir -p /toy-raft_instrumented
RUN /root/go/bin/antithesis-go-instrumentor /toy-raft /toy-raft_instrumented

# Build instrumented toy-raft
WORKDIR /toy-raft_instrumented/customer
RUN go mod download
RUN go build -C ./cmd/replica -v -a       -o raft-replica_no_race -tags enable_antithesis_sdk
RUN go build -C ./cmd/replica -v -a -race -o raft-replica_race    -tags enable_antithesis_sdk

# Install binary, library and symbols
FROM --platform=x86-64 ubuntu:latest
RUN mkdir -p /bin
COPY --from=builder /toy-raft_instrumented/customer/cmd/replica/raft-replica_no_race /bin/raft-replica_no_race
COPY --from=builder /toy-raft_instrumented/customer/cmd/replica/raft-replica_race /bin/raft-replica_race
COPY --from=builder /toy-raft_instrumented/symbols /symbols

# Link default server (referenced in entrypoint)
RUN ln -s /bin/raft-replica_race /bin/raft-replica

# Verify binary was compiled and instrumented properly
RUN /bin/raft-replica -help

# Install version and buildinfo files
RUN mkdir -p /opt/antithesis/
COPY ./antithesis/system-under-test/version /opt/antithesis/version
COPY ./antithesis/system-under-test/buildinfo /opt/antithesis/buildinfo

ENTRYPOINT ["/bin/raft-replica"]
