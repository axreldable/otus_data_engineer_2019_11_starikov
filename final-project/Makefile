run-infrustructure:
	docker-compose down && \
	docker-compose up --remove-orphans -d

deploy-flink-jobs:
	./input-adapter/deploy.sh >> ./input-adapter/job.log
	./flink-tweet-job/deploy.sh >> ./flink-tweet-job/job.log
	./output-adapter/deploy.sh >> ./output-adapter/job.log

run-flink:
	docker-compose -f docker-compose-flink.yaml down && \
	docker-compose -f docker-compose-flink.yaml up --remove-orphans -d && \
    ./deploy_jobs.sh

run-generator:
	docker-compose -f docker-compose-generator.yaml build && \
	docker-compose -f docker-compose-generator.yaml up -d

run-demo:
	sbt clean
	sbt assembly
	make run-infrustructure
#	make run-generator

stop-demo:
	docker-compose -f docker-compose-generator.yaml down
	docker-compose down
