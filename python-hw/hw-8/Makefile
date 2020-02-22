pull-vertica-image:
	docker pull dataplatform/docker-vertica

run-vertica:
	docker-compose up -d --remove-orphans

stop-vertica:
	docker-compose down

run-dbadmin:
	docker exec -ti -u dbadmin otus_hw_8_vertica bash
