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

### 코드 작성 예시
- 스텝바이스텝으로 설명
- 주석 및 코멘트를 잘달아줘