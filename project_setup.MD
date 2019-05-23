# Project Flink/Kafka setup
To setup on your local host, execute all commands in a new terminal.


## Start zookeeper

	wmctrl -r :ACTIVE: -N "Apache zookeeper"
	bin/zookeeper-server-start.sh config/zookeeper.properties

## Start kafka

	wmctrl -r :ACTIVE: -N "Kafka server"
	bin/kafka-server-start.sh config/server.properties

## Start api app.py

	wmctrl -r :ACTIVE: -N "API webserver"
	python3 app.py

## Extra
### Create kafka topics
 bin/kafka-topics.sh --create \
  --zookeeper localhost:2181 \
  --replication-factor 1 --partitions 1 \
  --topic user_in

 bin/kafka-topics.sh --create \
  --zookeeper localhost:2181 \
  --replication-factor 1 --partitions 1 \
  --topic user_out


### Listen to a topics
  bin/kafka-console-consumer.sh  --zookeeper localhost:2181 --topic user_out
  bin/kafka-console-producer.sh  --broker-list localhost:9092 --topic user_out
