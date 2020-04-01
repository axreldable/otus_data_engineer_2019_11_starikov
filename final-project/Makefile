create-network:
	docker network create ml-stream-engine-network

run-tweet-generation:
	sbt clean && \
	sbt assembly && \
	docker-compose down && \
	docker-compose up --remove-orphans -d && \
	./deploy_jobs.sh
