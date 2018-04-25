# metamorphosis
changing kafka

# setup kafka in your local machine
Run kafka in docker:
```bash
docker run --rm --env ADVERTISED_HOST=kafka --env ADVERTISED_PORT=9092 -p 2181:2181 -p 9092:9092 --name kafka -h kafka spotify/kafka
```

Testing your new `Kafka` broker:
1. Subscribe to `test` topic:
```bash
docker run --rm -it --name kafka-consumer --link kafka spotify/kafka /bin/sh -c '/opt/kafka_2.11-0.10.1.0/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic test'
```

2. Write and publish some messages to `test` topic:
```bash
docker run --rm -it --name kafka-producer --link kafka spotify/kafka /bin/sh -c 'echo type something... && /opt/kafka_2.11-0.10.1.0/bin/kafka-console-producer.sh --broker-list kafka:9092 --topic test'
```

# running the samza Job

```bash
echo "Preparing the samza Task..."
rm -rf target
mvn clean package
mkdir -p target/deploy/
tar -xvf target/simple-samza-job-1.0-SNAPSHOT-dist.tar.gz -C target/deploy/
```



```bash
./target/deploy/bin/run-job.sh \
  --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory \
  --config-path $PWD/config/local.simple-samza-job.properties
```

# References
***NOTE:*** Ensure the documentation you use is the relevant for your `Samza` version, now 0.14

https://samza.apache.org/learn/documentation/0.14/jobs/configuration.html
https://samza.apache.org/learn/documentation/0.14/jobs/packaging.html
https://samza.apache.org/learn/documentation/0.14/jobs/job-runner.html
