package org.example.kafka.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.kafka.model.OrderEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * Kafka Producer for Order Events
 * 주문 이벤트를 Kafka 토픽으로 전송하는 프로듀서
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderEventProducer {

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;
    
    @Value("${app.kafka.topic.order-events}")
    private String orderEventsTopic;

    /**
     * 주문 이벤트를 Kafka 토픽으로 비동기 전송
     * 
     * @param orderEvent 전송할 주문 이벤트
     */
    public void sendOrderEvent(OrderEvent orderEvent) {
        log.info("주문 이벤트 전송 시작 - orderId: {}, userId: {}", 
                 orderEvent.getOrderId(), orderEvent.getUserId());
        
        // 파티션 키로 userId 사용 (동일한 사용자의 메시지는 같은 파티션으로)
        CompletableFuture<SendResult<String, OrderEvent>> future = 
                kafkaTemplate.send(orderEventsTopic, orderEvent.getUserId(), orderEvent);
        
        // 비동기 콜백 설정
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("주문 이벤트 전송 성공 - orderId: {}, partition: {}, offset: {}", 
                         orderEvent.getOrderId(),
                         result.getRecordMetadata().partition(),
                         result.getRecordMetadata().offset());
            } else {
                log.error("주문 이벤트 전송 실패 - orderId: {}, error: {}", 
                          orderEvent.getOrderId(), ex.getMessage(), ex);
            }
        });
    }
}