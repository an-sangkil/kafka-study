package org.example.kafka.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderEvent {
    private String userId;
    private String orderId;
    private String productName;
    private LocalDateTime eventTimestamp;
    private String status;
    private Double amount;
}