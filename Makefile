GOOS=$(shell go env GOOS)
GOARCH=$(shell go env GOARCH)

build:
	mkdir -p dist/$(GOOS)-$(GOARCH)
	go build -o dist/$(GOOS)-$(GOARCH)/stan2js ./cmd/stan2js

zip:
	cd dist/$(GOOS)-$(GOARCH) && zip ../$(GOOS)-$(GOARCH).zip stan2js

dist:
	GOOS=linux GOARCH=amd64 make build zip
	GOOS=linux GOARCH=arm64 make build zip
	GOOS=darwin GOARCH=amd64 make build zip
	GOOS=darwin GOARCH=arm64 make build zip
	GOOS=windows GOARCH=amd64 make build zip
	GOOS=windows GOARCH=arm64 make build zip

.PHONY: dist
