
### final-project

Streaming engine to serve machine learning models.

#### Technologies

- [Apache Kafka](https://kafka.apache.org)
- [Apache Flink](https://flink.apache.org)
- [Apache Spark](https://spark.apache.org)
- Monitoring: [Prometheus](https://prometheus.io), [Grafana](https://grafana.com)

#### How to install
Requirements:
```
curl
git
docker
flink
```

#### how to run
```
./scripts/start.sh
```

#### how to stop
```
./scripts/stop.sh
```

#### how to monitor
Connect to Kafka on the localhost:9092 and use something like [Conductor](https://www.conduktor.io) to monitor messages.  
[Grafana UI](https://localhost:3000) with Kafka metrics  
[Prometheus UI](https://localhost:9090) with Kafka metrics  
[Flink cluster UI](https://localhost:8081)  

#### Architecture
![Architecture](https://github.com/axreldable/otus_data_engineer_2019_11_starikov/blob/master/final-project/images/otus-ml-streaming-system.png)
