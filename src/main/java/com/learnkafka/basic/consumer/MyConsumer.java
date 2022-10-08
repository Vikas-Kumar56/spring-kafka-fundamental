package com.learnkafka.basic.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.basic.dto.Customer;
import com.learnkafka.basic.event.MyEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class MyConsumer {

    private final ObjectMapper mapper;
    private final Logger logger = LoggerFactory.getLogger(MyConsumer.class);

    public MyConsumer(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @KafkaListener(
            id = "my-first-consumer",
            topics = "spring-topic",
            concurrency = "1",
            groupId = "first-group"
    )
    public void listen(
            @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
            @Header("trace_id") String traceId,
            @Header(KafkaHeaders.CONSUMER) Consumer<String, String> consumer,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) String partitionId,
            @Payload String payload
    ) throws JsonProcessingException {
//        System.out.println(consumer.groupMetadata().toString());
//        System.out.println("Key " + key + " traceId " + traceId);
        MyEvent<Customer> event = mapper.readValue(payload, new TypeReference<>() {});
//        System.out.println("Received new event: " + event.eventId());
        logger.info("""
                Key: {}
                PartitionId: {}
                ConsumerId: {}
                ThreadId: {}
                event: {}
                ====================================================================================
                """, key,partitionId, consumer.groupMetadata().memberId(), Thread.currentThread().getId(), event);
    }
}
