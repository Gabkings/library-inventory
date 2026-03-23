package com.gabkings.library_event_consumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gabkings.library_event_consumer.entity.LibraryEvent;
import com.gabkings.library_event_consumer.jpa.LibraryEventsRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@Slf4j
public class LibraryEventsService {

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    LibraryEventsRepository libraryEventsRepository;

    public void processLibraryEvent(ConsumerRecord<Integer,String> record) throws JsonProcessingException {

        LibraryEvent libraryEvent =
                objectMapper.readValue(record.value(), LibraryEvent.class);

        log.info("Library event received: {}", libraryEvent);

        switch (libraryEvent.getLibraryEventType()){
            case NEW :
                save(libraryEvent);
                break;
            case UPDATE :
                //validate the libraryevent
                validate(libraryEvent);
                save(libraryEvent);
                break;
            default:
                log.info("Unknown library event type: {}", libraryEvent.getLibraryEventType());
        }
    }

    private void validate(LibraryEvent libraryEvent) {
        if(libraryEvent.getLibraryEventId()==null){
            throw new IllegalArgumentException("Library Event Id is missing");
        }

        Optional<LibraryEvent> libraryEventOptional = libraryEventsRepository.findById(libraryEvent.getLibraryEventId());
        if(!libraryEventOptional.isPresent()){
            throw new IllegalArgumentException("Not a valid library Event");
        }
        log.info("Validation is successful for the library Event : {} ", libraryEventOptional.get());
    }

    private void save(LibraryEvent libraryEvent) {
        Integer bookId = libraryEvent.getBook().getBookId();

        Optional<LibraryEvent> existing =
                libraryEventsRepository.findById(bookId);

        if (existing.isPresent()) {
            LibraryEvent existingEvent = existing.get();

            existingEvent.setLibraryEventType(libraryEvent.getLibraryEventType());
            existingEvent.getBook().setBookName(libraryEvent.getBook().getBookName());
            existingEvent.getBook().setBookAuthor(libraryEvent.getBook().getBookAuthor());

            libraryEventsRepository.save(existingEvent);
        } else {
            libraryEvent.getBook().setLibraryEvent(libraryEvent);
            libraryEventsRepository.save(libraryEvent);
        }

    }

}
