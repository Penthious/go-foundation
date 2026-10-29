# buildinfo

[Overview](../README.md) · [Development and testing](../testsupport/README.md)

## Docker build metadata

`buildinfo.Current()` returns `Info{Version, Commit, BuildTime}` describing the
running service executable. The defaults are `dev`, `unknown`, and `unknown`.
Build time is informational text, conventionally a UTC RFC3339 timestamp. GOF
does not invoke git, inspect the container, or infer a build date at runtime.
Runtime environment variables cannot override embedded metadata.

Use Go linker string variables when building each service image. This fragment
belongs in the service's Go builder stage, after copying dependencies and source:

```dockerfile
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_TIME=unknown

RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags="-s -w \
      -X github.com/Penthious/go-foundation/buildinfo.version=${VERSION} \
      -X github.com/Penthious/go-foundation/buildinfo.commit=${COMMIT} \
      -X github.com/Penthious/go-foundation/buildinfo.buildTime=${BUILD_TIME}" \
    -o /out/service ./cmd/service
```

Replace `./cmd/service` with the service's main package. The `-X` targets use
**GOF's full module import path**, not the service's `main` package or an import
alias. Use whitespace-free versions/commit IDs and an
RFC3339 timestamp for these flags. See the official [Go linker options](https://pkg.go.dev/cmd/link).

```sh
docker build -t orders:1.2.3 \
  --build-arg VERSION=1.2.3 \
  --build-arg COMMIT=abc123 \
  --build-arg BUILD_TIME=2026-10-28T12:00:00Z .

docker run --rm -p 8080:8080 \
  -e ORDERS_SERVICE=orders \
  -e ORDERS_ENVIRONMENT=production \
  orders:1.2.3
```

Deployment environment stays runtime configuration, so the same image can move
from staging to production without changing its build identity. Pass version to
`logger.Config.Version` and `telemetry.Config.Version`; commit and build time can
be recorded in a startup log using the logger's string fields. Do not put secrets
in build metadata, `-ldflags`, or Docker build arguments: those values are part
of the artifact/build history. See [Docker build variables](https://docs.docker.com/build/building/variables/).
