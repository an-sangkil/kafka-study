package org.example.kafka.controller;

import lombok.RequiredArgsConstructor;
import org.example.kafka.model.OrderEvent;
import org.example.kafka.producer.OrderEventProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * 주문 이벤트를 생성하고 Kafka로 전송하는 REST API
 * 테스트 목적으로 간단한 주문 이벤트 생성 기능 제공
 */
@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderEventProducer orderEventProducer;

    /**
     * 주문 이벤트 생성 및 Kafka 전송
     * 
     * @param orderEvent 생성할 주문 이벤트 정보
     * @return 생성된 주문 이벤트 정보
     */
    @PostMapping
    public ResponseEntity<OrderEvent> createOrder(@RequestBody OrderEvent orderEvent) {
        // orderId가 없으면 자동 생성
        if (orderEvent.getOrderId() == null || orderEvent.getOrderId().isEmpty()) {
            orderEvent.setOrderId("ORDER-" + UUID.randomUUID().toString().substring(0, 8));
        }
        
        // eventTimestamp가 없으면 현재 시간으로 설정
        if (orderEvent.getEventTimestamp() == null) {
            orderEvent.setEventTimestamp(LocalDateTime.now());
        }
        
        // 기본 상태 설정
        if (orderEvent.getStatus() == null || orderEvent.getStatus().isEmpty()) {
            orderEvent.setStatus("CREATED");
        }
        
        // Kafka로 이벤트 전송
        orderEventProducer.sendOrderEvent(orderEvent);
        
        return ResponseEntity.ok(orderEvent);
    }

    /**
     * 대량 주문 이벤트 생성 (부하 테스트용)
     * 
     * @param count 생성할 주문 이벤트 개수
     * @param userId 사용자 ID (파티셔닝에 사용)
     * @return 생성 완료 메시지
     */
    @PostMapping("/bulk")
    public ResponseEntity<String> createBulkOrders(
            @RequestParam(defaultValue = "100") int count,
            @RequestParam(defaultValue = "test-user") String userId) {
        
        for (int i = 0; i < count; i++) {
            OrderEvent orderEvent = new OrderEvent();
            orderEvent.setOrderId("BULK-ORDER-" + i);
            orderEvent.setUserId(userId + "-" + (i % 10)); // 10명의 사용자로 분산
            orderEvent.setProductName("Product-" + (i % 5)); // 5개 상품으로 순환
            orderEvent.setEventTimestamp(LocalDateTime.now());
            orderEvent.setStatus("CREATED");
            orderEvent.setAmount(100.0 + (i % 100)); // 100~199 사이의 금액
            
            orderEventProducer.sendOrderEvent(orderEvent);
        }
        
        return ResponseEntity.ok(count + "개의 주문 이벤트가 생성되었습니다.");
    }
}