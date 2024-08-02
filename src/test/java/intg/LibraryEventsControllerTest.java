import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.library_events_producer.domain.Book;
import com.learnkafka.library_events_producer.domain.LibraryEvent;
import com.learnkafka.library_events_producer.domain.LibraryEventType;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import org.springframework.http.HttpHeaders;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = "library-events")
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
                    "spring.kafka.admin.properties.bootstrap-servers=${spring.embedded.kafka.brokers}"})
class LibraryEventsControllerTest {

    @Autowired
    TestRestTemplate restTemplate;

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    ObjectMapper objectMapper;

    private Consumer<Integer, String> consumer;

    @BeforeEach
    void setUp() {

        var configs = new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(),new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }


    @Test
    void testLibraryEvent() {

        HttpHeaders httpHeaders = new HttpHeaders();

        httpHeaders.set("content-type", MediaType.APPLICATION_JSON.toString());

        var libraryEventRecord = new LibraryEvent(11, LibraryEventType.UPDATE, new Book(11, "aa", "aa"));

        var httpEntity = new HttpEntity<>(libraryEventRecord, httpHeaders);



       var responseEntity =  restTemplate.exchange("/v1/libraryevent", HttpMethod.POST,
                                                    httpEntity,
                                                    LibraryEvent.class
                                                    );

       assertEquals(HttpStatus.CREATED, responseEntity.getStatusCode());

       ConsumerRecords<Integer, String> consumerRecords = KafkaTestUtils.getRecords(consumer);

       assert consumerRecords.count() == 1;

       consumerRecords.forEach(record ->
                                {
                                    LibraryEvent libraryEvent = null;
                                    try {
                                        libraryEvent = objectMapper.readValue(record.value(), LibraryEvent.class);
                                    } catch (JsonProcessingException e) {
                                        throw new RuntimeException(e);
                                    }
                                    assertEquals(libraryEvent, libraryEventRecord);
                                });

    }

}