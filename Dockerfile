FROM golang:1.21.0

WORKDIR /app

RUN export GO111MODULE=on

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN go build -o main .

EXPOSE 9000

CMD ["./main"]