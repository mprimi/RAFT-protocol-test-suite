
.PHONY: clean all

all: replica workload

clean:
	rm -f replica
	rm -f workload

replica:
	go build toy-raft/cmd/replica

workload:
	go build toy-raft/cmd/workload
