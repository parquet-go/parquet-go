# docker buildx build --progress plain --platform linux/s390x --tag pavolloffay/parque-go:s390x --build-arg=TARGETARCH=s390x -f ./Dockerfile .
FROM  golang:1.20 as builder

WORKDIR /workspace/parque
ADD ./ ./

RUN go env
RUN go test -v -trimpath -tags=purego
# RUN make test
