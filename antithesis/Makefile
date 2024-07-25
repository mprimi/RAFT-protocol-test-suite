.PHONY: run build build_sut build_workload build_config build_sut_servertests build_config_serverests all_server_tests server_smoke_tests pull_natsbox clean

# Uncomment if needed to debug build:
# DOCKER_BUILD_FLAGS = --no-cache

run: clean build pull_nats_server ./config/docker-compose.yml
	docker-compose -f ./config/docker-compose.yml --env-file ./config/environment up

build: build_workload build_sut build_config

build_sut: ./system-under-test/Dockerfile
	docker build $(DOCKER_BUILD_FLAGS) -f ./system-under-test/Dockerfile -t raft_node:latest ..

build_workload: ./workload/Dockerfile
	docker build $(DOCKER_BUILD_FLAGS) -f ./workload/Dockerfile -t workload:latest ..

build_config: ./config/Dockerfile
	docker build $(DOCKER_BUILD_FLAGS) -f ./config/Dockerfile -t config:latest ..

pull_nats_server:
	docker pull nats:latest
	docker tag nats:latest nats_server:latest

clean:
	docker-compose -f ./config/docker-compose.yml --env-file ./config/environment down
