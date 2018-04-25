#!/usr/bin/env bash
set -euxo pipefail

docker run --rm --env ADVERTISED_HOST=kafka --env ADVERTISED_PORT=9092 -p 2181:2181 -p 9092:9092 --name kafka -h kafka spotify/kafka

