package com.learnkafka.basic.dto;

import java.util.UUID;

public record Customer(
        UUID customerId,
        String name,
        String address
) {
}
