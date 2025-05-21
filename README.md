# Spanner Cassandra Go Client
> [!NOTE] 
> Spanner Cassandra Go Client is currently in public preview.

![latest release](https://img.shields.io/github/v/release/googleapis/go-spanner-cassandra)
![Go version](https://img.shields.io/github/go-mod/go-version/googleapis/go-spanner-cassandra)

## Introduction
The **Spanner Cassandra Go Client** is a tool designed to bridge applications written for the Apache Cassandra® database with Google Spanner. With Spanner's native support for the Cassandra v4 wire protocol, this client allows Go applications using the `gocql` driver, or even non-Go applications and tools like `cqlsh`, to connect seamlessly to a Spanner database.

This client acts as a local tcp proxy, intercepting the raw Cassandra protocol bytes sent by a driver or client tool. It then wraps these bytes along with necessary metadata into gRPC messages for communication with Spanner. Responses from Spanner are translated back into the Cassandra wire format and sent back to the originating driver or tool.

![in-process](in-process.png)

## Table of Contents

- [When to use spanner-cassandra?](#when-to-use-spanner-cassandra)
- [Prerequisites](#prerequisites)
- [Spanner Instructions](#spanner-instructions)
- [Getting started](#getting-started)
  - [In-Process Dependency](#in-process-dependency-recommended)
  - [Sidecar Proxy](#sidecar-proxy)
- [Options](#options)
- [Supported Cassandra Versions](#supported-cassandra-versions)
- [Unsupported Features](#unsupported-features)
- [License](#license)

## When to Use Spanner Cassandra Go Client?

This client is useful but not limited to the following scenarios:

* **Leveraging Spanner with Minimal Refactoring:** You want to use Spanner as the backend for your Go application but prefer to keep using the familiar `gocql` API for data access.
* **Using Non-Go Cassandra Tools:** You want to connect to Spanner using standard Cassandra tools like `cqlsh` or applications written in other languages that use Cassandra drivers.

## Prerequisites

You will need a [Google Cloud Platform Console][developer-console] project with the Spanner [API enabled][enable-api].
You will need to [enable billing][enable-billing] to use Google Spanner.
[Follow these instructions][create-project] to get your project set up.

Ensure that you run

```sh
gcloud auth application-default login
```

to set up your local development environment with authentication credentials.

Set the GCLOUD_PROJECT environment variable to your Google Cloud project ID:

```sh
gcloud config set project [MY_PROJECT_NAME]
```

## Spanner Instructions

- Database and all the tables should be created in advance before executing the queries against Spanner Cassandra Go Client.
- To migrate existing Cassandra schema to corresponding Spanner schema, refer to [spanner-cassandra-schema-tool](https://github.com/cloudspannerecosystem/spanner-cassandra-schema-tool) to automate this process.

## Getting Started

You can use `spanner-cassandra` in two main ways: as an **in-process dependency** within your Go application, or as a standalone **sidecar proxy** for other applications and tools.

* **In-Process Dependency:** Choose this method if you have a Go application already using `gocql` and want the spanner-cassandra client to run within the same process, providing a seamless switch to Spanner with minimal code modifications.
* **Sidecar Proxy:** Choose this method if your application is not written in Go, or if you want to use external Cassandra tools (like `cqlsh`) without modifying the application's code. The spanner-cassandra client runs as a separate process, intercepting network traffic.

### In-Process Dependency (Recommended)

For Go applications already using the `gocql` library, integrating the Spanner Cassandra Go Client requires only minor changes to the cluster initialization.

**Steps:**

*   Import the `spanner` package in your go applcation:

    ```go
    import spanner "github.com/googleapis/go-spanner-cassandra/cassandra/gocql"
    ```
*  Modify your cluster creation code. Instead of using `gocql.NewCluster`, use `spanner.NewCluster` and provide the Spanner database URI:

    ```go
    func main() {
      opts := &spanner.Options{
          // Required: Specify the Spanner database URI
          DatabaseUri: "projects/your_gcp_project/instances/your_spanner_instance/databases/your_spanner_database",
      }
      // Optional: Configure other gocql cluster settings as needed
      cluster := spanner.NewCluster(opts)
      cluster.Timeout = 5 * time.Second
      // Your Spanner database schema is mapped to a keyspace
      cluster.Keyspace = "your_spanner_database"
      // Important to close the resources
      defer spanner.CloseCluster(cluster)
      // Rest of your business logic
      session, err := cluster.CreateSession()
      if err != nil {
        fmt.Printf("Failed to create session: %v\n", err)
        return
      }
      defer session.Close()
      // Rest of your business logic such as session.Query(SELECT * FROM ...)
    }
    ```

*  Run your Go application as usual. The client will now route traffic to your Spanner database.

### Sidecar Proxy

![sidecar](sidecar.png)

For non-Go applications or tools like `cqlsh`, you can run the Spanner Cassandra Go Client as a standalone proxy.

**Method 1: Run locally with `go run`**

*  Clone the repository:

    ```bash
    git clone https://github.com/googleapis/go-spanner-cassandra.git
    cd go-spanner-cassandra
    ```

*  Run the `cassandra_launcher.go` with the required `-db` flag:

    ```bash
    go run cassandra_launcher.go -db "projects/your_gcp_project/instances/your_spanner_instance/databases/your_spanner_database" -tcp ":9042" -grpc-channels 4
    ```

    * Replace the value of `-db` with your Spanner database URI.
    * You can omit the `-tcp` to use the default `:9042` and omit `-grpc-channels` to use the default 4.

    See [Options](#options) for an explanation of all further options.

**Method 2: Run with pre-built docker image**

*  Pull from official registry repo:

    ```bash
    docker pull gcr.io/cloud-spanner-adapter/cassandra-adapter
    ```
*  Start the Spanner Cassandra Adapter Docker container:

    ```bash
    export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
    docker run -d -p 9042:9042 \
    -e GOOGLE_APPLICATION_CREDENTIALS \
    -v ${GOOGLE_APPLICATION_CREDENTIALS}:${GOOGLE_APPLICATION_CREDENTIALS}:ro \
    gcr.io/cloud-spanner-adapter/cassandra-adapter \
    -db projects/your-project/instances/your-instance/databases/your-database
    ```
    See [Options](#options) for an explanation of all further options.

## Options

The following list contains the most frequently used startup options for Spanner Cassandra Client.

```
-db <DatabaseUri>
  * The Spanner database URI (required). This specifies the Spanner database that the client will connect to.
  * Example: projects/your-project/instances/your-instance/databases/your-database

-tcp <TCPEndpoint>
  * The client proxy listener address. This defines the TCP endpoint where the client will listen for incoming Cassandra client connections.
  * Default:
    * When running in-process inside Golang applicaion: localhost:9042
    * When running as a sidecar proxy: :9042 to bind all network interfaces, suitable for Docker forwarding.

-grpc-channels <NumGrpcChannels>
  * The number of gRPC channels to use when connecting to Spanner.
  * Default: 4

-log <LogLevel>
  * Log level used by the global zap logger.
  * Default: info
```

## Supported Cassandra Versions

By default, Spanner Cassandra client communicates using the [Cassandra 4.0 protocol](https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec) and is fully tested and verified with **Cassandra 4.x**, providing complete support. For **Cassandra 3.x**, the client is designed to be compatible and should work seamlessly, though we recommend thorough testing within your specific setup.

## Unsupported Features

* named parameters
* pagination
* ScanCAS

## License

[Apache License 2.0](LICENSE)

[developer-console]: https://console.developers.google.com/
[enable-api]: https://console.cloud.google.com/flows/enableapi?apiid=spanner.googleapis.com
[enable-billing]: https://cloud.google.com/apis/docs/getting-started#enabling_billing
[create-project]: https://cloud.google.com/resource-manager/docs/creating-managing-projects
[cloud-cli]: https://cloud.google.com/cli

