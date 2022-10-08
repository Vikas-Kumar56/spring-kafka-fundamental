package com.learnkafka.basic.exception;

public class RetryableException extends RuntimeException {

    public RetryableException(String message) {
        super(message);
    }
}
