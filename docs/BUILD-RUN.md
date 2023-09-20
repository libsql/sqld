# Build and run libsql-server

There are four ways to build and run libsql-server:

- [Download a prebuilt binary](#download-a-prebuilt-binary)
- [Using Homebrew](#build-and-install-with-homebrew)
- [Using a prebuilt Docker image](#using-a-prebuilt-docker-image)
- [From source using Docker/Podman](#build-from-source-using-docker--podman)
- [From source using Rust](#build-from-source-using-rust)

## Running libsql-server

You can simply run launch the executable with no command line arguments to run
an instance of libsql-server. By default, libsql-server listens on 127.0.0.1 port 8080 and
persists database data in a directory `./data.sqld`.

Use the `--help` flag to discover how to change its runtime behavior.

## Query libsql-server

You can query libsql-server using one of the provided [client
libraries](../#client-libraries).

You can also use the sqlite3 CLI to query the SQLite 3 compatible database file
managed by libsql-server.

```bash
sqlite3 ./data.sqld/data
```

Be sure to stop libsql-server before using `sqlite3` like this.

## Download a prebuilt binary

The [libsql-server releases page] for this repository lists released versions of libsql-server
along with downloads for macOS and Linux.

## Build and install with Homebrew

The libsql-server formulae for Homebrew works with macOS, Linux (including WSL).

### 1. Add the tap `libsql/libsql-server` to Homebrew

```bash
brew tap libsql/libsql-server
```

### 2. Install the formulae `libsql-server`

```bash
brew install libsql-server
```

This builds and installs the binary `libsql-server` into `$HOMEBREW_PREFIX/bin/libsql-server`,
which should be in your PATH.

### 3. Verify that `libsql-server` works

```bash
libsql-server --help
```

## Using a prebuilt Docker image

The libsql-server release process publishes a Docker image to the GitHub Container
Registry. The URL is https://ghcr.io/libsql/libsql-server. You can run the latest image locally
on port 8080 with the following:

```bash
docker run -p 8080:8080 -d ghcr.io/libsql/libsql-server:latest
```

Or you can run a specific version using one of the [libsql-server container release
tags] in the following form for version X.Y.Z:

```bash
docker run -p 8080:8080 -d ghcr.io/libsql/libsql-server:vX.Y.Z
```

## Build from source using Docker / Podman

To build libsql-server with Docker, you must have a Docker [installed] and running on
your machine with its CLI in your shell PATH.

[installed]: https://docs.docker.com/get-docker/

### 1. Clone this repo

Clone this repo using your preferred mechanism. You may want to use one of the
[libsql-server release tags].

Change to the `libsql-server` directory.

### 2. Build with Docker

Run the following to build a Docker image named "libsql/libsql-server" tagged with
version "latest".

```bash
docker build -t libsql/libsql-server:latest .
```

### 3. Verify the build

Check that libsql-server built successfully using its --help flag:

```bash
docker container run \
  --rm \
  -i \
  libsql/libsql-server \
  /bin/libsql-server --help
```

### 4. Create a data volume

The following will create a volume named `libsql-data` that libsql-server uses to persist
database files.

```bash
docker volume create libsql-data
```

### 5. Run libsql-server in a container

The following uses the built image to create and run a new container named
`libsql-server`, attaching the `libsql-data` volume to it, and exposing its port 8080
locally:

```bash
docker container run \
  -d \
  --name libsql-server \
  -v libsql-data:/var/lib/libsql-server\
  -p 127.0.0.1:8080:8080 \
  libsql/libsql-server:latest
```

8080 is the default port for the libsql-server HTTP service that handles client queries.
With this container running, you can use the URL `http://127.0.0.1:8080` or
`ws://127.0.0.1:8080` to configure one of the libSQL client SDKs for local
development.

### 6. Configure libsql-server with environment variables

In the libsql-server output using `--help` from step 3, you saw the names of command line
flags along with the names of environment variables (look for "env:") used to
configure the way libsql-server works.

## Build from source using Rust

To build from source, you must have a Rust development environment installed and
available in your PATH.

Currently we only support building libsql-server on macOS and Linux (including WSL). We
are working native Windows build instructions.

### 1. Setup

Install dependencies:

```bash
./scripts/install-deps.sh
```

### 2. Clone this repo

Clone this repo using your preferred mechanism. You may want to use one of the
[libsql-server release tags].

Change to the `libsql-server` directory.

Install git submodules:

```bash
git submodule update --init --force --recursive --depth 1
```

### 3. Build with cargo

```bash
cargo build
```

The libsql-server binary will be in `./target/debug/libsql-server`.

### 4. Verify the build

Check that libsql-server built successfully using its --help flag:

```bash
./target/debug/libsql-server --help
```

### 5. Run libsql-server with all defaults

The following starts libsql-server, taking the following defaults:

- Local files stored in the directory `./data.sqld`
- Client HTTP requests on 127.0.0.1:8080

```bash
./target/debug/libsql-server
```

8080 is the default port for the libsql-server HTTP service that handles client queries.
With this container running, you can use the URL `http://127.0.0.1:8080` or
`ws://127.0.0.1:8080` to configure one of the libSQL client SDKs for local
development.

### 6. Run tests (optional)

```console
make test
```
[libsql-server releases page]: https://github.com/libsql/libsql-server/releases
[libsql-server container release tags]: https://github.com/libsql/libsql-server/pkgs/container/libsql-server
[libsql-server release tags]: https://github.com/libsql/libsql-server/releases
