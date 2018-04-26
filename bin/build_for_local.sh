#!/usr/bin/env bash
set -euxo pipefail

echo "Preparing the samza Task..."
rm -rf target
mvn clean package
mkdir -p target/deploy/
tar -xf target/simple-samza-job-1.0-SNAPSHOT-dist.tar.gz -C target/deploy/
