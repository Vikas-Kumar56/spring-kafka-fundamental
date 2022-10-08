package com.learnkafka.basic.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

@Component
public class NonBlockingConsumer {

    private static final Logger logger  = LoggerFactory.getLogger(NonBlockingConsumer.class);

    @RetryableTopic(
      backoff = @Backoff(value = 100L, multiplier = 2)
    )
    @KafkaListener(
            id = "retryable-consumer",
            groupId = "retryable-group",
            topics = "retryable-topic"
    )
    public void listen(
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.DELIVERY_ATTEMPT) int delivery,
            @Header(KafkaHeaders.GROUP_ID) String groupId,
            @Payload String payload
    ) {
        logger.info("""
                topic: {}
                ThreadId: {}
                deliveryAttempt: {}
                payload: {}
                groupId: {}
                """, topic, Thread.currentThread().getId(), delivery, payload, groupId);

        throw new RuntimeException("some error occurred!");
    }
}
