package com.gabkings.library_event_producer.unit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gabkings.library_event_producer.controller.LibraryEventsController;
import com.gabkings.library_event_producer.domain.LibraryEvent;
import com.gabkings.library_event_producer.integ.util.TestUtil;
import com.gabkings.library_event_producer.producer.LibraryEventProducer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;






@WebMvcTest(LibraryEventsController.class)
@Import(LibraryEventsControllerUnitTest.TestConfig.class)
class LibraryEventsControllerUnitTest {



    @Autowired
    MockMvc mockMvc;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    LibraryEventProducer  eventProducer;

    @BeforeEach
    void setUp() {
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void postLibraryEvent() throws Exception {
        var json = objectMapper.writeValueAsString(TestUtil.libraryEventRecord());

        when(eventProducer.sendLibraryEvent3(isA(LibraryEvent.class)))
                .thenReturn(null);

        mockMvc
                .perform(MockMvcRequestBuilders.post("/v1/libraryevent")
                        .content(json)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isCreated());
    }

    @Test
    void postLibraryEvent_4xx() throws Exception {
        //given

        LibraryEvent libraryEvent = TestUtil.libraryEventRecordWithInvalidBook();

        String json = objectMapper.writeValueAsString(libraryEvent);
        when(eventProducer.sendLibraryEvent2(isA(LibraryEvent.class))).thenReturn(null);
        //expect
        String expectedErrorMessage = "book.bookId - must not be null, book.bookName - must not be blank";
        mockMvc.perform(post("/v1/libraryevent")
                        .content(json)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().is4xxClientError())
                .andExpect(content().string(expectedErrorMessage));

    }

    @Test
    void updateLibraryEvent() throws Exception {

        //given


        String json = objectMapper.writeValueAsString(TestUtil.libraryEventRecordUpdate());
        when(eventProducer.sendLibraryEvent2(isA(LibraryEvent.class))).thenReturn(null);

        //expect
        mockMvc.perform(
                        put("/v1/libraryevent")
                                .content(json)
                                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());

    }

    @Test
    void updateLibraryEvent_withNullLibraryEventId() throws Exception {

        //given

        String json = objectMapper.writeValueAsString(TestUtil.libraryEventRecordUpdateWithNullLibraryEventId());
        when(eventProducer.sendLibraryEvent2(isA(LibraryEvent.class))).thenReturn(null);

        //expect
        mockMvc.perform(
                        put("/v1/libraryevent")
                                .content(json)
                                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().is4xxClientError())
                .andExpect(content().string("Please pass the LibraryEventId"));

    }

    @Test
    void updateLibraryEvent_withNullInvalidEventType() throws Exception {

        //given

        String json = objectMapper.writeValueAsString(TestUtil.newLibraryEventRecordWithLibraryEventId());
        //when(libraryEventProducer.sendLibraryEvent_Approach2(isA(LibraryEvent.class))).thenReturn(null);

        //expect
        mockMvc.perform(
                        put("/v1/libraryevent")
                                .content(json)
                                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().is4xxClientError())
                .andExpect(content().string("Only UPDATE event type is supported"));

    }

    @Test
    void postLibraryEvent2() {
    }

    @Test
    void postLibraryEvent3() {
    }


    @TestConfiguration
    static class TestConfig {

        @Bean
        public LibraryEventProducer eventProducer() {
            return Mockito.mock(LibraryEventProducer.class);
        }
    }


}