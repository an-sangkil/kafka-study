### 기술스택
- spring 3.5.5
- java 25
- kafka 3.9
- k8s keda(hpa) (컨슈머 leg 수집)
  - leg가 1000 이상 벌어졌을때 컨슈머 확장



### 구현 내용
- 파티션 전략을 이용한 동시성 보장
- k8s를 사용한 컨슈머 자동 스케일링

### 리퀴스트 샘플
약 2kb 에서 3kb 의 json
```json
{
  "userId": "user-A",
  "orderId": "order-101",
  "productName": "Laptop",
  "eventTimestamp": "2023-10-27T10:00:00Z"
  //   ... 생략
}
```

### 실행 방법

#### 단일 노드 (개발용)
```bash
# Kafka 단일 노드 시작
docker-compose up -d

# 애플리케이션 실행
./gradlew bootRun
```

#### 클러스터 모드 (운영 환경 테스트)
```bash
# Kafka 클러스터 시작 (3개 노드)
docker-compose -f compose-cluster.yaml up -d

# 애플리케이션 설정에서 bootstrap servers 변경
# application.properties에서 아래와 같이 수정:
# spring.kafka.bootstrap-servers=localhost:9092,localhost:9093,localhost:9094
```

#### API 테스트
```bash
# 단일 주문 생성
curl -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-123","productName":"Laptop","amount":1500.0}'

# 대량 주문 생성 (부하 테스트)
curl -X POST "http://localhost:8080/api/orders/bulk?count=2000&userId=test-user"

# Kafka UI 접속: http://localhost:8080
```

### 코드 작성 예시
- 스텝바이스텝으로 설명
- 주석 및 코멘트를 잘달아줘