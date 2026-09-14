# syntax=docker/dockerfile:1

# Sample commands to build and run locally:

# docker build -t spanner-cassandra-adapter:latest .

# docker run -p 9042:9042 \
# -e GOOGLE_APPLICATION_CREDENTIALS="/tmp/keyfile/key.json" \
# -v /your/path/key.json:/tmp/keyfile/key.json \
# spanner-cassandra-adapter \
# --database-uri projects/your-project/instances/your-instance/databases/your-database

FROM golang:1.25-alpine AS builder

# Set the working directory inside the container to the project root
WORKDIR /app

# Download Go modules
COPY go.mod go.sum ./
RUN go mod download

# Copy the entire project source code into the container
COPY . .

# Change the working directory to the location of cassandra_launcher.go
WORKDIR /app

# Build
RUN go build -o cassandra_launcher

# Use a smaller clean runtime image
FROM alpine:latest AS runtime

# Copy the binary from the builder stage
COPY --from=builder /app/cassandra_launcher /cassandra_launcher

# Expose the application default port(can be override at runtime)
EXPOSE 9042

ENTRYPOINT ["/cassandra_launcher"]

# Set the CMD to enable passing golang flags (ie: --database-uri)
CMD []
