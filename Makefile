all: build

build:
	go build -ldflags "\
	-X main.gitRepo=$$(git remote get-url --push origin) \
	-X main.gitCommit=$$(git rev-list -1 HEAD) \
	-X main.appVersion=$$(git tag -l | tail -n1) \
	-X main.buildStamp=$$(date -u '+%Y-%m-%d')" \
	-o gridfs2s3

build_linux:
	GOOS="linux" GOARCH="amd64" go build -ldflags "\
	-X main.gitRepo=$$(git remote get-url --push origin) \
    	-X main.gitCommit=$$(git rev-list -1 HEAD) \
    	-X main.appVersion=$$(git tag -l | tail -n1) \
    	-X main.buildStamp=$$(date -u '+%Y-%m-%d')" \
    	-o gridfs2s3.linux

clean:
	rm gridfs2s3


startdb:
	docker run --rm -d -p 27017:27017 --name gridfs mongo:3.6-jessie

stopdb:
	docker stop gridfs