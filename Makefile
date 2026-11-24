.PHONY: build run test clean docker

build:
	go build -o app .

run:
	go run .

test:
	go test ./...

clean:
	rm -f app

docker:
	docker compose up --build