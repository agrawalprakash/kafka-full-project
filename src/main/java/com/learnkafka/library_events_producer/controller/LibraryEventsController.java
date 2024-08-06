package com.learnkafka.library_events_producer.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.library_events_producer.domain.LibraryEvent;
import com.learnkafka.library_events_producer.domain.LibraryEventType;
import com.learnkafka.library_events_producer.producer.LibraryEventsProducer;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.Nullable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@RestController
@Slf4j
public class LibraryEventsController {

    private final LibraryEventsProducer libraryEventsProducer;

    public LibraryEventsController(LibraryEventsProducer libraryEventsProducer) {
        this.libraryEventsProducer = libraryEventsProducer;
    }

    @PostMapping("/v1/libraryEvent")
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {


        log.info("Response entity is " + libraryEvent);

        libraryEventsProducer.sendLibraryEvent_approach3(libraryEvent);
        // invoke the Kafka producer
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);

        // bootstrap-servers
        // key-serializer
        // value-serializer
    }

    @PutMapping("/v1/libraryEvent")
    public ResponseEntity<?> putLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException,TimeoutException {


        ResponseEntity<String> BAD_REQUEST = validateUpdateRequest(libraryEvent);

        if (BAD_REQUEST != null) return BAD_REQUEST;

        libraryEventsProducer.sendLibraryEvent_approach3(libraryEvent);



        return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);

    }

    @Nullable
    private static ResponseEntity<String> validateUpdateRequest(LibraryEvent libraryEvent) {
        if (libraryEvent.libraryEventId() == null) {

            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please pass the library event ID");
        }

        if (! libraryEvent.libraryEventType().equals(LibraryEventType.UPDATE)) {

            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Only UPDATE type is supported");
        }
        return null;
    }
}
