# One image for every part (gateway, backend, overadmit, health), chosen by the command.
# Build context: the repo root.
FROM golang:1.26.10-bookworm AS build
WORKDIR /src
# Dependencies first, in their own layer, so editing code doesn't download them again.
COPY go.mod go.sum ./
RUN go mod download
# Only the Go sources: keeps .git and docs out, so editing a README doesn't rebuild.
COPY cmd/ cmd/
COPY internal/ internal/
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/rategate ./cmd/rategate

# A static binary needs no OS: distroless has no shell or package manager, so very little
# to attack, and it runs as a non-root user.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/rategate /rategate
USER nonroot:nonroot
ENTRYPOINT ["/rategate"]
