FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o ingest-api .

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /app/ingest-api .
CMD ["./ingest-api"]
