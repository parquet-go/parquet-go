# docker buildx build --progress plain --platform linux/s390x --tag user/parque-go:s390x --build-arg=TARGETARCH=s390x -f ./.github/workflows/Dockerfile .
FROM  golang:1.22 as builder

WORKDIR /workspace/parque
ADD ../.. ./

RUN go env
RUN go test -v -trimpath