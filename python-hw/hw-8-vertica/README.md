# Задание по Vertica

## How to run Vertica
```
make pull-vertica-image
make run-vertica
make run-dbadmin
vsql -hlocalhost -Udbadmin
```

## How to stop Vertica
```
make stop-vertica
```

## How check hw
```
make pull-vertica-image
make run-vertica
docker cp ./data otus_hw_8_vertica:/home/dbadmin/
make run-dbadmin
vsql -hlocalhost -Udbadmin
```
