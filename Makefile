all:
	go build *.go
run1:
	./workload 1000 102400
run3:
	./workload 1000 307200
run5:
	./workload 1000 502000
clear:
	rm workload

