package com.gabkings.library_event_producer.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.gabkings.library_event_producer.domain.LibraryEvent;
import com.gabkings.library_event_producer.domain.LibraryEventType;
import com.gabkings.library_event_producer.producer.LibraryEventProducer;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@RestController
@RequestMapping("/v1/libraryevent")
public class LibraryEventsController {

    private final LibraryEventProducer libraryEventProducer;

    public LibraryEventsController(LibraryEventProducer libraryEventProducer) {
        this.libraryEventProducer = libraryEventProducer;
    }

    @PostMapping
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException {
        // invoke the kafka producer
        libraryEventProducer.sendLibraryEvent(libraryEvent);
        return new ResponseEntity<>(libraryEvent, HttpStatus.CREATED);
    }

    @PostMapping("/v2")
    public ResponseEntity<LibraryEvent> postLibraryEvent2(@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
        // invoke the kafka producer
        libraryEventProducer.sendLibraryEvent2(libraryEvent);
        return new ResponseEntity<>(libraryEvent, HttpStatus.CREATED);
    }

    @PostMapping("/v3")
    public ResponseEntity<LibraryEvent> postLibraryEvent3(@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
        // invoke the kafka producer
        libraryEventProducer.sendLibraryEvent3(libraryEvent);
        return new ResponseEntity<>(libraryEvent, HttpStatus.CREATED);
    }

    @PutMapping("")
    public ResponseEntity<?> putLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {


        ResponseEntity<String> BAD_REQUEST = validateLibraryEvent(libraryEvent);
        if (BAD_REQUEST != null) return BAD_REQUEST;

        libraryEventProducer.sendLibraryEvent2(libraryEvent);
        return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
    }

    private static ResponseEntity<String> validateLibraryEvent(LibraryEvent libraryEvent) {
        if (libraryEvent.libraryEventId() == null) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please pass the LibraryEventId");
        }

        if (!LibraryEventType.UPDATE.equals(libraryEvent.libraryEventType()))  {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Only UPDATE event type is supported");
        }
        return null;
    }
}
