
##### 
###### 수행환경 
- mac
- local 
- rancher-desktop 

###### ADVERTISED LISTENERS 설정
Local에서  docker-kafka에 접속할때는 localhost:9092,9093,9094 로 열어준다.  
- KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-1:29092,PLAINTEXT_HOST://localhost:9092

Kubernetes pod 에서 docker-kafka에 접속할때는 docker의 내부 경로를 뜻하는 'host.docker.internal' 을 설정해야 kubernetes 의 pod에서 접근 가능하다.
- KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-1:29092,PLAINTEXT_HOST://host.docker.internal:9092

##### docker compose 실해 명령어 
```shell
docker-compose up -d -f ./compose-cluster.yaml
```