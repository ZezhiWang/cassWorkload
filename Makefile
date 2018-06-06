all:
	go build *.go
run:
	./workload 1000
clear:
	rm workload

