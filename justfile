# note: call scripts from /scripts

test:
    go test ./...

vet:
    go vet ./...

build:
    go build -o out/ ./...

clean:
    rm -rf out/
