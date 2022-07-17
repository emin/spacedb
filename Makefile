executable = db

build:
	go build -o $(executable) cmd/db_cmd.go

winbuild: 
	GOOS=windows go build cmd/db_cmd.go
clean: 
	rm -f ./$(executable)

wipe: clean
	rm -rf ./test-db && mkdir ./test-db

test: build
	go clean -testcache && go test -v ./...

run: build
	./$(executable)

profile:
	go test -bench=. ./skiplist
