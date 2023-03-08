FROM golang:1.20.1-bullseye AS build

WORKDIR /mnt/homework

ENV CGO_ENABLED 0
COPY go.* .
RUN  go mod download
COPY . .

RUN go build -o /mnt/homework/homework-object-storage cmd/gateway/main.go

# Docker is used as a base image so you can easily start playing around in the container using the Docker command line client.
FROM docker

COPY --from=build /mnt/homework/homework-object-storage /usr/local/bin/homework-object-storage

ENTRYPOINT [ "/usr/local/bin/homework-object-storage" ]
