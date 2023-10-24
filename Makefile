.PHONY: build

tools:
	bash ./scripts/tools.sh

tests:
	TEST_PACKAGE=./... bash ./scripts/tests.sh

tests-verbose:
	TEST_VERBOSE=true TEST_LOG_FORMAT=standard-verbose TEST_PACKAGE=./... bash ./scripts/tests.sh

lint:
	bash ./scripts/lint.sh

fix:
	bash ./scripts/fix.sh

ci: lint tests

build-processor:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ./build/target/processor ./cmd/processor/main.go
	ls -lh ./build/target

build-cli:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ./build/target/cli ./cmd/cli/main.go
	ls -lh ./build/target

build-cli-all:
	CGO_ENABLED=0 GOOS=linux   GOARCH=amd64 go build -o ./build/target/cli_linux_amd64   ./cmd/cli/main.go
	CGO_ENABLED=0 GOOS=linux   GOARCH=arm64 go build -o ./build/target/cli_linux_arm64   ./cmd/cli/main.go
	CGO_ENABLED=0 GOOS=darwin  GOARCH=amd64 go build -o ./build/target/cli_macos_amd64   ./cmd/cli/main.go
	CGO_ENABLED=0 GOOS=darwin  GOARCH=arm64 go build -o ./build/target/cli_macos_arm64   ./cmd/cli/main.go
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o ./build/target/cli_win_amd64.exe ./cmd/cli/main.go
	CGO_ENABLED=0 GOOS=windows GOARCH=arm64 go build -o ./build/target/cli_win_arm64.exe ./cmd/cli/main.go
	ls -lh ./build/target