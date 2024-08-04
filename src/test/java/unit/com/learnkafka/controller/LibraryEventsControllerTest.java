package com.learnkafka.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.library_events_producer.controller.LibraryEventsController;
import com.learnkafka.library_events_producer.domain.Book;
import com.learnkafka.library_events_producer.domain.LibraryEvent;
import com.learnkafka.library_events_producer.domain.LibraryEventType;
import com.learnkafka.library_events_producer.producer.LibraryEventsProducer;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.OngoingStubbing;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(LibraryEventsController.class)
class LibraryEventsControllerTest {

    @Autowired
    MockMvc mockMvc;

    @Autowired
    ObjectMapper objectMapper;

    @MockBean
    LibraryEventsProducer libraryEventsProducer;

    @Test
    void postLibraryEvent() throws Exception {

        var json = objectMapper.writeValueAsString(
                new LibraryEvent(11, LibraryEventType.UPDATE, new Book(11, "aa", "aa")));

        when(libraryEventsProducer.sendLibraryEvent_approach3(isA(LibraryEvent.class))).thenReturn(null);

        mockMvc.perform(MockMvcRequestBuilders.post("/v1/libraryEvent").content(json).contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isCreated());

    }

    @Test
    void postLibraryEvent_inValid() throws Exception {

        var json = objectMapper.writeValueAsString(
                new LibraryEvent(11, LibraryEventType.UPDATE, new Book(null, null, null)));

        when(libraryEventsProducer.sendLibraryEvent_approach3(isA(LibraryEvent.class))).thenReturn(null);

        mockMvc.perform(MockMvcRequestBuilders.post("/v1/libraryEvent").content(json).contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().is4xxClientError());

    }

}