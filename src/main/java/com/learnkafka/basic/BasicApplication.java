package com.learnkafka.basic;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.basic.dto.Customer;
import com.learnkafka.basic.event.MyEvent;
import com.learnkafka.basic.exception.NotRetryableException;
import com.learnkafka.basic.exception.RetryableException;
import com.learnkafka.basic.producer.MyProducer;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.BackOffExecution;
import org.springframework.util.backoff.FixedBackOff;

import java.sql.Array;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.random.RandomGenerator;

@EnableKafka
@SpringBootApplication
public class BasicApplication {

    public static void main(String[] args) {
        SpringApplication.run(BasicApplication.class, args);
    }

    @Bean
    public NewTopic topic() {
        return TopicBuilder.name("spring-topic")
                .partitions(3)
                .replicas(1)
                .build();
    }

	@Bean
	public NewTopic errorTopic() {
		return TopicBuilder.name("error-topic")
				.partitions(3)
				.replicas(1)
				.build();
	}

    @Bean
    public NewTopic retryableTopic() {
        return TopicBuilder.name("retryable-topic")
                .partitions(3)
                .replicas(1)
                .build();
    }


    @Bean
    public ApplicationRunner runner(MyProducer producer) {
        return args -> {
            publishToRetryableTopic(producer);
//			startSimpleProducer(producer);
//            startProducer(producer);
        };
    }

    private void publishToRetryableTopic(MyProducer producer) {
        producer.sendMessage("retryable-topic", "RetryableEvent-1");
    }

	private void startSimpleProducer(MyProducer producer) {
        producer.sendMessage("error-topic", "some message - 1");
        producer.sendMessage("error-topic", "some message - 2");
        producer.sendMessage("error-topic", "some message - 3");
	}

    private void startProducer(MyProducer producer) throws JsonProcessingException, InterruptedException {
        var customerId_1 = UUID.randomUUID();
        var customerId_2 = UUID.randomUUID();
        var customerId_3 = UUID.randomUUID();
        UUID[] customerIds = new UUID[]{customerId_1, customerId_2, customerId_3};

        while (true) {
            // 0, 1, 2
            int index = ThreadLocalRandom.current().nextInt(0, 3);
            System.out.println("index " + index);
            Customer customer = new Customer(
                    customerIds[index],
                    "fake customer name",
                    "Any address"
            );

            MyEvent<Customer> event = new MyEvent<>(
                    UUID.randomUUID(),
                    customer,
                    Timestamp.from(Instant.now())
            );

            producer.sendMessage("spring-topic", customer.customerId(), event);

            Thread.sleep(10000L);
        }
    }

    @Bean
    public ObjectMapper mapper() {
        return new ObjectMapper();
    }

    @Bean
    public KafkaListenerErrorHandler errorHandler() {
        return (message, exception) -> {
            System.out.println("inside error handler" + message);
            throw exception;
//            return "FAILED";
        };
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory) {
        final ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(defaultErrorHandler());

        factory.getContainerProperties().setDeliveryAttemptHeader(true);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }

    @Bean
    public DefaultErrorHandler defaultErrorHandler() {
       var defaultErrorHandler = new DefaultErrorHandler(
               (consumerRecord, exception) -> {
                  System.out.println("inside DefaultErrorHandler");
               },
               new FixedBackOff(1000L, 2)
       );

       defaultErrorHandler.setAckAfterHandle(false);

       defaultErrorHandler.addNotRetryableExceptions(
               NotRetryableException.class
       );

       defaultErrorHandler.addRetryableExceptions(
               RetryableException.class
       );

       defaultErrorHandler.setCommitRecovered(false);

       return defaultErrorHandler;
    }

}
