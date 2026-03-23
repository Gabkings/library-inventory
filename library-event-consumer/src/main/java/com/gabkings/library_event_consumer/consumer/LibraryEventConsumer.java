package com.gabkings.library_event_consumer.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.gabkings.library_event_consumer.service.LibraryEventsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class LibraryEventConsumer {

    @Autowired
    LibraryEventsService libraryEventsService;


    @KafkaListener(topics = {"library-events"}, groupId = "group1")
    public void onMessage(ConsumerRecord<Integer, String> record) throws JsonProcessingException {

        log.info("Consumer received record: {}", record.value());

        libraryEventsService.processLibraryEvent(record);

        log.info("Library event saved successfully");


    }


}
