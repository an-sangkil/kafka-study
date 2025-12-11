# 기술 문서 (TECH.md)

## 시스템 아키텍처

### 전체 구성도
```
┌─────────────────┐    HTTP    ┌─────────────────┐    Kafka    ┌─────────────────┐
│     Client      │ ─────────→ │  Spring Boot    │ ─────────→  │  Kafka Cluster  │
│   (REST API)    │            │   Application   │             │   (3 brokers)   │
└─────────────────┘            └─────────────────┘             └─────────────────┘
                                        │                              │
                                        ▼ consume                     ▼ metrics
                               ┌─────────────────┐             ┌─────────────────┐
                               │ Order Consumer  │             │      KEDA       │
                               │  (비즈니스 로직)  │             │  (Auto Scaler)  │
                               └─────────────────┘             └─────────────────┘
                                        │                              │
                                        ▼ process                     ▼ scale
                               ┌─────────────────┐             ┌─────────────────┐
                               │   Database      │             │   Kubernetes    │
                               │   (Optional)    │             │     Pods        │
                               └─────────────────┘             └─────────────────┘
```

## Kafka KRaft 모드 상세

### KRaft vs Zookeeper 비교

| 구분 | Zookeeper 모드 | KRaft 모드 |
|-----|---------------|------------|
| **메타데이터 저장** | 외부 Zookeeper | 내부 Raft 로그 |
| **복잡도** | 높음 (별도 클러스터) | 낮음 (통합) |
| **지연시간** | 높음 | 낮음 |
| **확장성** | 제한적 | 우수 |
| **운영 부담** | 높음 | 낮음 |

### 포트 구성 및 통신 흐름

#### 단일 노드 모드 (`compose.yaml`)
```
┌─────────────────┐
│   kafka:9092    │ ← 호스트 접근용
│     │           │
│   29092 ←────── │ ← 내부 통신용  
│     │           │
│   29093 ←────── │ ← 컨트롤러용
└─────────────────┘
```

#### 클러스터 모드 (`compose-cluster.yaml`)
```
Host:9092 ──→ ┌─────────────┐ 29092 ─┐
Host:9093 ──→ │   kafka-1   │ ←──────┼─── 브로커 간 복제
Host:9094 ──→ └─────────────┘ 29093 ─┘
                     │                ↑
                     ▼ Raft 합의       │ 컨트롤러 통신
              ┌─────────────┐         │
              │   kafka-2   │ ────────┤
              └─────────────┘         │
                     │                │
                     ▼ Raft 합의       │
              ┌─────────────┐         │
              │   kafka-3   │ ────────┘
              └─────────────┘
```

### KRaft 핵심 설정 설명

#### 1. 프로세스 역할 (`KAFKA_PROCESS_ROLES`)
```yaml
KAFKA_PROCESS_ROLES: 'broker,controller'
```
- **broker**: 데이터 저장 및 클라이언트 요청 처리
- **controller**: 메타데이터 관리 (토픽, 파티션, 리더 선출)
- **Combined Mode**: 하나의 노드가 두 역할 동시 수행

#### 2. 쿼럼 투표자 (`KAFKA_CONTROLLER_QUORUM_VOTERS`)
```yaml
KAFKA_CONTROLLER_QUORUM_VOTERS: '1@kafka-1:29093,2@kafka-2:29093,3@kafka-3:29093'
```
- Raft 합의 알고리즘 참여 노드 정의
- 과반수(2/3) 동의로 메타데이터 변경 결정
- 홀수 개 노드 권장 (Split-brain 방지)

#### 3. 리스너 설정
```yaml
# 수신 대기 포트
KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:29092,PLAINTEXT_HOST://0.0.0.0:9092,CONTROLLER://0.0.0.0:29093

# 클라이언트 광고 주소
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-1:29092,PLAINTEXT_HOST://localhost:9092

# 프로토콜 매핑
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT,CONTROLLER:PLAINTEXT
```

## KEDA 자동 스케일링

### 동작 원리
```
┌─────────────────┐     lag 모니터링    ┌─────────────────┐
│      KEDA       │ ←─────────────────→ │  Kafka Cluster  │
│   (Scaler)      │                     │                 │
└─────────────────┘                     └─────────────────┘
         │                                       │
         ▼ Pod 스케일링                           ▼ 메트릭
┌─────────────────┐                     ┌─────────────────┐
│   Kubernetes    │                     │ Consumer Group  │
│      HPA        │                     │ (order-processing│
└─────────────────┘                     │     -group)     │
                                        └─────────────────┘
```

### 스케일링 트리거 설정
```yaml
triggers:
- type: kafka
  metadata:
    bootstrapServers: kafka:9092
    consumerGroup: order-processing-group
    topic: order-events
    lagThreshold: '1000'  # lag > 1000이면 스케일 아웃
```

### 스케일링 시나리오
1. **정상 상태**: Consumer lag < 1000 → Pod 1개
2. **부하 증가**: 대량 메시지 유입 → lag > 1000
3. **스케일 아웃**: KEDA가 Pod 2~10개로 확장
4. **부하 감소**: lag < 1000 → 5분 후 스케일 다운

## Spring Kafka 구성

### Producer 설정
```java
@Service
public class OrderEventProducer {
    // 파티션 키: userId (동일 사용자 메시지는 같은 파티션)
    kafkaTemplate.send(topic, orderEvent.getUserId(), orderEvent);
}
```

### Consumer 설정
```java
@KafkaListener(topics = "order-events")
public void handleOrderEvent(OrderEvent orderEvent, Acknowledgment ack) {
    // 비즈니스 로직 처리
    processOrder(orderEvent);
    
    // 수동 커밋 (처리 완료 후)
    ack.acknowledge();
}
```

### 파티션 전략
```
userId: "user-A" ──→ Partition 0 ──→ Consumer Pod 1
userId: "user-B" ──→ Partition 1 ──→ Consumer Pod 2  
userId: "user-C" ──→ Partition 2 ──→ Consumer Pod 3
```
- **동시성 보장**: 같은 사용자 주문은 순서 보장
- **부하 분산**: 여러 파티션으로 처리량 증대

## 메시지 흐름

### 1. 메시지 생성
```
HTTP POST /api/orders
    ↓
OrderController.createOrder()
    ↓
OrderEventProducer.sendOrderEvent()
    ↓
Kafka Topic: order-events
```

### 2. 메시지 소비
```
Kafka Topic: order-events
    ↓
@KafkaListener
    ↓
OrderEventConsumer.handleOrderEvent()
    ↓
비즈니스 로직 처리
    ↓
acknowledge() (수동 커밋)
```

### 3. 자동 스케일링
```
대량 메시지 ──→ Consumer Lag 증가 ──→ KEDA 감지 ──→ Pod 증가
     ↓               ↓                    ↓
   처리량 증가 ←─── Lag 감소 ←───────── 더 많은 Consumer
```

## 모니터링 및 관리

### Kafka UI 대시보드
- **URL**: http://localhost:8080
- **기능**: 토픽, 파티션, 컨슈머 그룹 모니터링
- **메트릭**: 처리량, lag, 오프셋 등

### 주요 모니터링 지표
1. **Consumer Lag**: 미처리 메시지 수
2. **Throughput**: 초당 처리 메시지 수  
3. **Partition Count**: 파티션별 메시지 분산
4. **Pod Count**: KEDA에 의한 스케일링 상태

## 성능 최적화

### 처리량 향상
- **파티션 수 증가**: 병렬 처리 향상
- **배치 처리**: 여러 메시지 묶어서 처리
- **비동기 처리**: Non-blocking I/O 활용

### 지연시간 최적화
- **수동 커밋**: 처리 완료 후 커밋
- **리밸런싱 최소화**: 안정된 컨슈머 그룹 유지
- **로컬 캐싱**: 중복 처리 방지

## 장애 대응

### 브로커 장애
- **복제 팩터 3**: 데이터 손실 방지
- **Min In-Sync Replicas 2**: 안정성 확보
- **자동 리더 선출**: 가용성 유지

### 컨슈머 장애
- **Health Check**: 비정상 Pod 자동 재시작
- **Dead Letter Queue**: 처리 실패 메시지 격리
- **Retry Logic**: 일시적 오류 재시도

## 배포 전략

### 로컬 개발
```bash
docker-compose up -d          # 단일 노드
./gradlew bootRun            # 애플리케이션
```

### 클러스터 테스트
```bash
docker-compose -f compose-cluster.yaml up -d  # 3노드 클러스터
# application.properties 수정 후 실행
```

### Kubernetes 배포
```bash
docker build -t kafka-keda-skeleton .
kubectl apply -f k8s/
```