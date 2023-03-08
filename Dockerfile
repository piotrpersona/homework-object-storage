FROM golang:1.20.1-bullseye AS build

WORKDIR /mnt/homework

COPY go.* .
RUN go mod download
COPY . .

RUN go build  -ldflags="-w -s" -o /mnt/homework/homework-object-storage main.go

# Docker is used as a base image so you can easily start playing around in the container using the Docker command line client.
FROM docker
COPY --from=build /mnt/homework/homework-object-storage /usr/local/bin/homework-object-storage

ENTRYPOINT [ "/usr/local/bin/homework-object-storage" ]
