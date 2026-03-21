package com.gabkings.library_event_producer.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gabkings.library_event_producer.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
@Slf4j
public class LibraryEventProducer {

    KafkaTemplate<Integer, String> kafkaTemplate;
    ObjectMapper objectMapper;

    @Value("${spring.kafka.topic}")
    public String topic;

    public LibraryEventProducer(KafkaTemplate<Integer, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {
        var key = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent);

        /*
        * Behind the scene working
        * 1. blocking call - get metadata about the kafka cluster
        * 2. Send message happens - Returns a CompletableFuture
        * */

        var completableFuture = kafkaTemplate.send(topic,key, value);

        completableFuture
                .whenComplete((event, throwable) -> {
                    if (throwable != null) {
                        handleFailure(key, value, throwable);
                    }else {
                        handleSuccess(key, value, event);
                    }
                });
    }

    public SendResult<Integer, String> sendLibraryEvent2(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
        var key = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent);

        /*
         * Behind the scene working
         * 1. blocking call - get metadata about the kafka cluster
         * 2. Send message happens - Returns a CompletableFuture
         * */

        var sendResult = kafkaTemplate.send(topic,key, value).get(3, TimeUnit.SECONDS);

        handleSuccess(key, value, sendResult);

        return sendResult;
    }

    public SendResult<Integer, String> sendLibraryEvent3(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
        var key = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent);

        /*
         * Behind the scene working
         * 1. blocking call - get metadata about the kafka cluster
         * 2. Send message happens - Returns a CompletableFuture
         * */

        var producerRecord =  buildProducerRecord(key, value);

        var sendResult = kafkaTemplate.send(producerRecord).get(3, TimeUnit.SECONDS);

        handleSuccess(key, value, sendResult);

        return sendResult;
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> event) {
        log.info("Successfully send library event to kafka topic for key {} and value {} and partition {}", key, value, event.getRecordMetadata().partition());
    }

    private void handleFailure(Integer key, String value, Throwable throwable) {
        log.error("Error sending the message {} ", throwable.getMessage(), throwable);
    }

    private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value) {
        List<Header> recordHeaders = List.of(new RecordHeader("event-source", "scanner".getBytes(StandardCharsets.UTF_8)));

        return new ProducerRecord<>(topic,null, key, value, recordHeaders);
    }


}
