# This Makefile locally builds the images and stands up the system as described
# in the config image docker-compose.

# Uncomment if needed to debug build:
# DOCKER_BUILD_FLAGS = --no-cache

.PHONY: run
run: clean build pull_nats_server ./config/docker-compose.yml
	docker-compose -f ./config/docker-compose.yml --env-file ./config/environment up

.PHONY: build
build: build_workload build_sut build_config

.PHONY: build_sut
build_sut: ./system-under-test/Dockerfile
	docker build $(DOCKER_BUILD_FLAGS) -f ./system-under-test/Dockerfile -t raft_sut:latest ..

.PHONY: build_workload
build_workload: ./workload/Dockerfile
	docker build $(DOCKER_BUILD_FLAGS) -f ./workload/Dockerfile -t raft_workload:latest ..

.PHONY: build_config
build_config: ./config/Dockerfile
	docker build $(DOCKER_BUILD_FLAGS) -f ./config/Dockerfile -t raft_config:latest ..

.PHONY: pull_nats_server
pull_nats_server:
	docker pull nats:latest
	docker tag nats:latest nats_server:latest

.PHONY: clean
clean:
	docker-compose -f ./config/docker-compose.yml --env-file ./config/environment down
