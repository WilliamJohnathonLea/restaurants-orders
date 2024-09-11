APP_NAME = orders
BUILD_DIR = ./build
BIN = $(BUILD_DIR)/$(APP_NAME)

run:
	$(BIN)

test:
	go test ./...

build:
	mkdir -p $(BUILD_DIR)
	go build -o $(BIN) cmd/main.go

clean:
	rm -rf $(BUILD_DIR)

.PHONY: run test build clean
