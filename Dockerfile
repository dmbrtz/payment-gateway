FROM golang:1.26.5 AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG SERVICE

RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/app ./cmd/${SERVICE}

FROM alpine:3.20

WORKDIR /app

COPY --from=builder /bin/app /app/app
COPY migrations /app/migrations

CMD ["/app/app"]