# Задание по Vertica

## How to run Vertica
```
make run-vertica
make run-dbadmin
vsql -hlocalhost -Udbadmin
```

## How to stop Vertica
```
make stop-vertica
```

## How check hw
1. Run Vertica and vsql console
```
make run-vertica
make run-dbadmin
vsql -hlocalhost -Udbadmin
```
2. Create tables from `hw-ddl-script.sql` script
3. Create data marts from `hw-dml-script.sql` script
