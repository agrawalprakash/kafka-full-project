package com.learnkafka.library_events_producer.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.library_events_producer.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
@Slf4j
public class LibraryEventsProducer {

    @Value("${spring.kafka.topic}")
    private String topic;

    private final KafkaTemplate<Integer, String> kafkaTemplate;

    private final ObjectMapper objectMapper;
    public LibraryEventsProducer(KafkaTemplate<Integer, String> kafkaTemplate, ObjectMapper objectMapper) {

        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;

    }

    public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {

        var key = libraryEvent.libraryEventId();

        var value = objectMapper.writeValueAsString(libraryEvent);

        var completableFuture = kafkaTemplate.send(topic, key, value);

        completableFuture.whenComplete((sendResult, throwable) ->
                {
                    if (throwable != null) {

                        handleFailure( key, value, throwable );

                    } else {

                        handleSuccess(key, value, sendResult);

                    }
                }
        );

    }

    public SendResult<Integer, String> sendLibraryEvent_approach2(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {

        var key = libraryEvent.libraryEventId();

        var value = objectMapper.writeValueAsString(libraryEvent);

        SendResult<Integer, String> sendResult = kafkaTemplate.send(topic, key, value).get(3, TimeUnit.SECONDS);

        handleSuccess(key, value, sendResult);

        return sendResult;

    }

    public void sendLibraryEvent_approach3(LibraryEvent libraryEvent) throws JsonProcessingException {

        var key = libraryEvent.libraryEventId();

        var value = objectMapper.writeValueAsString(libraryEvent);
        
        var producerRecord = buildProducerRecord(key, value);

        var completableFuture = kafkaTemplate.send(producerRecord);

        completableFuture.whenComplete((sendResult, throwable) ->
                {
                    if (throwable != null) {

                        handleFailure( key, value, throwable );

                    } else {

                        handleSuccess(key, value, sendResult);

                    }
                }
        );

    }

    private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value) {

        List<Header> recordHeader = List.of(new RecordHeader("event-source", "scanner".getBytes()));

        ProducerRecord producerRecord = new ProducerRecord(topic, key, value, recordHeader);

        return producerRecord;

    }


    private void handleSuccess(Integer key, String value, SendResult<Integer, String> sendResult) {

        log.info(" Printing {} {} {} ", key, value, sendResult.getRecordMetadata().partition());
    }

    private void handleFailure(Integer key, String value, Throwable throwable) {

        log.error("Sending message to kafka had this exception {}", throwable.getMessage(), throwable);
    }

}
