package org.example.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.example.kafka.model.OrderEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

/**
 * Kafka Consumer for Order Events
 * 주문 이벤트를 소비하여 비즈니스 로직 처리
 * KEDA에서 컨슈머 lag를 모니터링하여 자동 스케일링
 */
@Slf4j
@Service
public class OrderEventConsumer {

    /**
     * 주문 이벤트 처리
     * - 파티션별로 병렬 처리 가능
     * - 수동 커밋으로 처리 완료 후 offset 커밋
     * 
     * @param orderEvent 처리할 주문 이벤트
     * @param partition 메시지가 속한 파티션 번호
     * @param offset 메시지 오프셋
     * @param acknowledgment 수동 커밋을 위한 객체
     */
    @KafkaListener(
        topics = "${app.kafka.topic.order-events}",
        groupId = "${spring.kafka.consumer.group-id}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleOrderEvent(
            @Payload OrderEvent orderEvent,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {
        
        log.info("주문 이벤트 수신 시작 - orderId: {}, userId: {}, partition: {}, offset: {}"
                ,  orderEvent.getOrderId(), orderEvent.getUserId(), partition, offset);
        
        try {
            // 비즈니스 로직 처리
            processOrderEvent(orderEvent);
            
            // 처리 완료 후 수동 커밋
            acknowledgment.acknowledge();
            
            log.info("주문 이벤트 처리 완료 - orderId: {}, userId: {}",  orderEvent.getOrderId(), orderEvent.getUserId());
            
        } catch (Exception ex) {
            log.error("주문 이벤트 처리 실패 - orderId: {}, error: {}",  orderEvent.getOrderId(), ex.getMessage(), ex);
            // 에러 발생 시 재시도 로직이나 DLQ 전송 등의 처리 가능
            throw ex; // 재시도를 위해 예외를 다시 던짐
        }
    }

    /**
     * 실제 비즈니스 로직 처리
     * 
     * @param orderEvent 처리할 주문 이벤트
     */
    private void processOrderEvent(OrderEvent orderEvent) {
        // TODO: 실제 비즈니스 로직 구현
        // 예: 주문 상태 업데이트, 재고 확인, 결제 처리 등
        
        // 처리 시간 시뮬레이션 (실제 환경에서는 제거)
        try {
            Thread.sleep(100); // 100ms 처리 시간
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        log.info("비즈니스 로직 처리 완료 - 주문: {}, 상품: {}, 금액: {}", orderEvent.getOrderId(), orderEvent.getProductName(), orderEvent.getAmount());
    }
}