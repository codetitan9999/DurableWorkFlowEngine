FROM golang:1.23-alpine AS builder

WORKDIR /workspace

COPY go.mod go.sum ./
RUN go mod download

COPY apps ./apps
COPY internal ./internal
COPY migrations ./migrations

RUN CGO_ENABLED=0 GOOS=linux go build -o /out/worker ./apps/worker

FROM alpine:3.20

WORKDIR /app

COPY --from=builder /out/worker /app/worker
COPY --from=builder /workspace/migrations /app/migrations

EXPOSE 8081

CMD ["/app/worker"]
