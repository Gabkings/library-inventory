package com.gabkings.library_event_producer.integ;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gabkings.library_event_producer.domain.LibraryEvent;
import com.gabkings.library_event_producer.integ.util.TestUtil;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.*;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}"})
class LibraryEventsControllerIntegrationTest {

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate testRestTemplate;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    EmbeddedKafkaBroker  embeddedKafkaBroker;

    private Consumer<Integer, String> consumer;


//    String url = "http://localhost:" + port + "/v1/library_events";


    @BeforeEach
    void setUp() {
        var configs = KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "latest");
        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);

    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    @Test
    void postLibraryEvent() throws JsonProcessingException {
        LibraryEvent libraryEvent = TestUtil.libraryEventRecord();
        System.out.println("libraryEvent : " + objectMapper.writeValueAsString(libraryEvent));
        HttpHeaders headers = new HttpHeaders();

        headers.set("content-type", MediaType.APPLICATION_JSON.toString());

        String url = "http://localhost:" + port + "/v1/libraryevent/v2";
        var httpEntity = new HttpEntity<>(TestUtil.libraryEventRecord(),headers);
        var responseEntity = testRestTemplate
                .exchange(url, HttpMethod.POST, httpEntity, LibraryEvent.class);
                //.exchange(url, HttpMethod.POST,httpEntity, LibraryEvent.class);

        assertEquals(HttpStatus.CREATED, responseEntity.getStatusCode());

        ConsumerRecords<Integer, String> record = KafkaTestUtils.getRecords(consumer);

        assert record.count() == 1;

        record.forEach(record1 -> {
            var libraryEventRecord = TestUtil.parseLibraryEventRecord(objectMapper, record1.value());
//            System.out.println("Library event +++++++>>>>> : "+ libraryEventRecord);
            assertEquals(libraryEvent, libraryEventRecord);
        });

    }

    @Test
    void postLibraryEvent2() {
    }

    @Test
    void postLibraryEvent3() {
    }
}