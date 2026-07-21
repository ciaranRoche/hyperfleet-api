# Expects the statically-linked `controller` binary to be built first:
#   CGO_ENABLED=0 go build -o controller .
FROM scratch
COPY controller /controller
USER 65534
ENTRYPOINT ["/controller"]
