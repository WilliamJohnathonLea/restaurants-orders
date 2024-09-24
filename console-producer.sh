#! /bin/zsh

docker exec -it kafka_broker kafka-console-producer --bootstrap-server kafka.service:9092 --topic $1
