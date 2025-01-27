# docker buildx build --progress plain --platform linux/s390x --tag user/parque-go:s390x --build-arg=TARGETARCH=s390x -f ./.github/workflows/Dockerfile .
FROM  golang:1.23 AS builder

WORKDIR /workspace/parquet
RUN go env

ADD ../../go.mod .
ADD ../../go.sum .
RUN go mod download

ADD ../.. ./
RUN CGO_ENABLED=0 go test -trimpath ./...
