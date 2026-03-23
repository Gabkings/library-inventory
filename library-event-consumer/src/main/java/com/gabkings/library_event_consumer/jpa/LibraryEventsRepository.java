package com.gabkings.library_event_consumer.jpa;

import com.gabkings.library_event_consumer.entity.LibraryEvent;
import org.springframework.data.repository.CrudRepository;

public interface LibraryEventsRepository extends CrudRepository<LibraryEvent,Integer> {
}
