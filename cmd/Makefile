containerbin:
	GOOS=linux GOARCH=386 CGO_ENABLED=0 go build -o pgpublish
	docker build -t xtracdev/pgpublish:latest .
