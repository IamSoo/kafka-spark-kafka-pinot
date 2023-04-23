# kafka-spark-kafka-pinot


### How to run
```commandline
docker-compose up
```
It will run the spark cluster, master and worker, a zookeeper and a kafka

### Create topics
Exec into the kafka controller and run the following 

```commandline
kafka-topics.sh --create --replication-factor 1 --bootstrap-server localhost:9092 --topic test_topic
kafka-topics.sh --create --replication-factor 1 --bootstrap-server localhost:9092 --topic cars_sales
```


### Run the spark Submit job and use a console consumer to look into the data
Exec into the spark container and run the following
``
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 /src/streaming/test_stream.py
``
Check the data inside kafka
```
kafka-console-consumer.sh --topic test_topic --bootstrap-server localhost:9092
```

Execute the command
```commandline
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 /src/streaming/send_messages_to_kafka.py
```

Check the data inside kafka
```commandline
kafka-console-consumer.sh --topic cars_sales --bootstrap-server localhost:9092

```