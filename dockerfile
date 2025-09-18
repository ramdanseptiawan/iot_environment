# Gunakan image Go
FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build binary
RUN go build -o hello-go main.go

# Stage final
FROM alpine:3.20

WORKDIR /root/

COPY --from=builder /app/hello-go .

EXPOSE 3000

CMD ["./hello-go"]
