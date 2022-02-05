SHELL = /bin/sh
CURRENT_UID := $(shell id -u)

build:
	DOCKER_BUILDKIT=1 docker-compose build

rebuild:
	DOCKER_BUILDKIT=1 docker-compose build --no-cache

up:
	docker-compose up -d --force-recreate
