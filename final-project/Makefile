create-network:
	docker network create ml-stream-engine-network

run-tweet-generation:
	sbt assembly
	docker-compose up --build

run-structured-consumer:
	docker-compose -f docker-compose-structured-consumer.yaml build
	docker-compose -f docker-compose-structured-consumer.yaml up