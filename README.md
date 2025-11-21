# Cách 1: pull rồi run container

pull 3 tech stack từ dockerhub

docker run -d --name cassandra -p 9042:9042 cassandra:4.2

docker run -d --name mysql_dw `
  -p 3307:3306 `
  -e MYSQL_ROOT_PASSWORD=rootpass `
  -e MYSQL_DATABASE=dw `
  -e MYSQL_USER=dw_user `
  -e MYSQL_PASSWORD=dw_pass `
  mysql:latest

docker run -d --name spark `
  -p 8080:8080 `
  -v D:/2025/Docker_cassandra_mysql_spark/etl:/etl `
  spark:latest /bin/bash -c "start-master.sh"


