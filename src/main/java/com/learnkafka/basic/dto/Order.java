package com.learnkafka.basic.dto;

import java.sql.Timestamp;
import java.util.UUID;

public record Order(
        UUID orderId,
        UUID productId,
        UUID customerId,
        Timestamp creationDate,
        int quantity
) {
}
