package com.learnkafka.basic.consumer;

import com.learnkafka.basic.exception.RetryableException;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class ErrorCaseConsumer {

    @KafkaListener(
            id = "error-consumer",
            topics = "error-topic",
            groupId = "error-group",
            errorHandler = "errorHandler"
    )
    public void listen(
            @Header(KafkaHeaders.DELIVERY_ATTEMPT) int delivery,
            @Payload String payload,
            @Header(KafkaHeaders.ACKNOWLEDGMENT) Acknowledgment acknowledgment,
            @Header(KafkaHeaders.CONSUMER) Consumer<String, String> consumer
            ) throws InterruptedException {
        System.out.println("payload " + payload + delivery);
        Thread.sleep(1000L);
        if(delivery >= 3) {
            consumer.pause(consumer.assignment());
        } else {
            throw new RetryableException("some error occurred");
        }

    }
}
