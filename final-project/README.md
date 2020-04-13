
### final-project

Streaming engine to serve machine learning models.

#### Technologies

- [Apache Kafka](https://kafka.apache.org)
- [Apache Flink](https://flink.apache.org)
- [Apache Spark](https://spark.apache.org)
- Monitoring: [Prometheus](https://prometheus.io), [Grafana](https://grafana.com)

#### How to install on GCP
Requirements:
```
curl
git
docker:
curl -sSL https://get.docker.com/ | sh
sudo curl -L "https://github.com/docker/compose/releases/download/1.25.4/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
```

1. Run google cloud machine
2. 
```
git clone https://github.com/axreldable/otus_data_engineer_2019_11_starikov.git
cd otus_data_engineer_2019_11_starikov/final-project/

```



#### how to run
```
sbt assembly
```
