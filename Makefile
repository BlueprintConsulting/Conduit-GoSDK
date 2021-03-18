.PHONY: default
default: displayhelp ;

displayhelp:
	@echo Use "clean, showcoverage, tests, build, buildlinux or run" with make, por favor.

showcoverage: tests
	@echo Running Coverage output
	go tool cover -html=coverage.out

tests: clean
	@echo Running Tests
	go test --coverprofile=coverage.out ./...

docker:
	docker build -t conduit-gosdk:latest . -f Dockerfile
	docker run -it --env CONDUIT_SERVER=${CONDUIT_SERVER} --env CONDUIT_TOKEN=${CONDUIT_TOKEN} conduit-gosdk:latest

run: build
	@echo Running program
	CONDUIT_SERVER=${CONDUIT_SERVER} CONDUIT_TOKEN=${CONDUIT_TOKEN} ./Conduit-GoSDK

build: clean
	@echo Running build command
	go build

buildlinux:
	env GOOS=linux go build -ldflags="-s -w"

clean:
	@echo Removing binary TODO
	rm -rf ./bin ./vendor Gopkg.lock
