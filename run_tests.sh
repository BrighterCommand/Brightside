#!/usr/bin/env bash

docker-compose -f tests-docker-compose.yml up -d --build
docker exec -it $(docker ps -aqf "name=brightside-tests") python testrunner.py
docker-compose -f tests-docker-compose.yml down

