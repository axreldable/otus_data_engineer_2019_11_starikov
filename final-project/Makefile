create-network:
	docker network create ml-stream-engine-network

run-infrustructure:
	sbt clean && \
	sbt assembly && \
	docker-compose down && \
	docker-compose up --remove-orphans -d

run-deploy-flink-jobs:
	./deploy_jobs.sh

run-flink:
	docker-compose -f docker-compose-flink.yaml down && \
	docker-compose -f docker-compose-flink.yaml up --remove-orphans -d && \
    ./deploy_jobs.sh


run-generator:
	docker-compose -f docker-compose-generator.yaml down && \
    docker-compose -f docker-compose-generator.yaml up --remove-orphans -d
