containerbin:
	GOOS=linux GOARCH=386 CGO_ENABLED=0 go build -o genagg
	docker build -t xtracdev/genagg:latest .

clean:
	rm -f genagg
	
