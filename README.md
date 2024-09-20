# Escli

Escli is a tool for interacting with an Elasticsearch service via the command line.

This project began as an experimental Python side project during November 2021, implementing a limited set of functionality.
It has been reimplemented from scratch in 2024, this time in Rust, using the newly-updated Rust client due for GA release in 2025.

The project is still experimental, and may not yet be suitable for production use.


## Scope

The tool is not intended to provide access to the full Elasticsearch API.
Instead, it offers a curated set of core functions that are guaranteed to be available across all Elasticsearch offerings (serverless, cloud, on-prem).

Specifically, the following areas of functionality are currently included:

- Search
- Bulk ingestion
- Index management
- Service information
- Ping utility


## Installation

No distributions are currently available, but the project may be used directly from source code.
You will require git and a Rust compiler stack (e.g. via rustup).

Clone the repository:
```bash
$ git@github.com:technige/escli.git
```

Build in debug mode:
```bash
$ cargo build
```

Or build in release mode with optimisation:
```bash
$ cargo build --release
```

This will generate an executable called `escli`.


## Addressing & Authentication

The `escli` tool relies on connection details and credentials supplied through environment variables.
The `ESCLI_URL` variable is required for addressing.
For API key authentication, set the `ESCLI_API_KEY` variable;
alternatively, for user and password authentication, `ESCLI_USER` and `ESCLI_PASSWORD` can be used.

The following variables are accepted:

### `ESCLI_URL`
The URL to which to connect in the form `scheme://host:port`.
Both `http` and `https` schemes are valid here.

### `ESCLI_API_KEY`
The API key used for authentication over HTTP.
This can be used as an alternative to user/password auth (below).

### `ESCLI_USER`
The name of the user, used for authentication over HTTP.
If this value is not set, `elastic` is used as a default.

### `ESCLI_PASSWORD`
The password used for authentication over HTTP.
This can be used as an alternative to API key auth (above).


## Checking connectivity with `ping`

Escli provides a `ping` command to check server connectivity.
This operates in a similar way to the standard command line `ping` utility, but instead sends an HTTP `HEAD` request to the service root.

```bash
$ escli ping --count 4`
HEAD http://localhost:9200/
200 OK: seq=1 time=1.975005ms
200 OK: seq=2 time=3.250639ms
200 OK: seq=3 time=1.017053ms
200 OK: seq=4 time=2.836599ms
```


## Fetching Elasticsearch system details with `info`

The `info` command queries the backend service to gather naming and version information.

```bash
$ escli info`
Name: d36e2bed1b74
Cluster Name: docker-cluster
Cluster UUID: IYeaY0mcQ5mGWBEzJoWDgA
Version:
  Number: 8.15.1
  Build Flavor: default
  Build Type: docker
  Build Hash: 253e8544a65ad44581194068936f2a5d57c2c051
  Build Date: 2024-09-02T22:04:47.310170297Z
  Build Snapshot: false
  Lucene Version: 9.11.1
  Minimum Wire Compatibility Version: 7.17.0
  Minimum Index Compatibility Version: 7.0.0
Tagline: You Know, for Search
```
