

##### docker kafka topic 생성
```shell
# 토픽 정보확인
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# 인풋 토픽 
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic input-topic --partitions 1 --replication-factor 1
# 아웃풋 토픽 
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic output-topic --partitions 1 --replication-factor 1

```