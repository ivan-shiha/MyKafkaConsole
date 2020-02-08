# MyKafkaConsole

## This project abstracts KafkaProducer and KafkaConsumer. Runs on top of Confluent.Kafka package.

### How to Run:

1. start both local kafka broker on port 9092 and zookeeper on port 2181
```bash
docker-compose up
```

2. check kafdrop ui
```bash
http://localhost:9000/
```

3. start kafka producer from visual studio and follow the instructions on console (*check appSettings for config)

4. start kafka consumer from visual studio and follow the instructions on console (*check appSettings for config)