FROM golang:1.23-alpine AS builder

WORKDIR /workspace

COPY go.mod go.sum ./
RUN go mod download

COPY apps ./apps
COPY internal ./internal
COPY migrations ./migrations

RUN CGO_ENABLED=0 GOOS=linux go build -o /out/api ./apps/api

FROM alpine:3.20

WORKDIR /app

COPY --from=builder /out/api /app/api
COPY --from=builder /workspace/migrations /app/migrations

EXPOSE 8080

CMD ["/app/api"]
