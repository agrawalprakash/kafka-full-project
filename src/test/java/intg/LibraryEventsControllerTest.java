import com.learnkafka.library_events_producer.domain.Book;
import com.learnkafka.library_events_producer.domain.LibraryEvent;
import com.learnkafka.library_events_producer.domain.LibraryEventType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
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
    

    @Test
    void testLibraryEvent() {

        HttpHeaders httpHeaders = new HttpHeaders();

        httpHeaders.set("content-type", MediaType.APPLICATION_JSON.toString());

        var httpEntity = new HttpEntity<>(new LibraryEvent(11, LibraryEventType.UPDATE, new Book(11, "aa", "aa"))
                                          , httpHeaders);



       var responseEntity =  restTemplate.exchange("/v1/libraryevent", HttpMethod.POST,
                                                    httpEntity,
                                                    LibraryEvent.class
                                                    );

       assertEquals(HttpStatus.CREATED, responseEntity.getStatusCode());
    }

}