executable = db

build:
	go build -o $(executable) cmd/db_cmd.go

winbuild: 
	GOOS=windows go build cmd/db_cmd.go
clean: 
	rm -f ./$(executable)

test: build
	go clean -testcache && go test -v ./...

run: build
	./$(executable)

profile:
	go test -bench=. ./skiplist

clean_delta:
	rm -f ./delta

build_delta: clean_delta
	go build -o delta cmd/delta_encoding.go

delta: build_delta
	./delta