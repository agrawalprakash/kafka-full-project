package com.learnkafka.library_events_producer.domain;

import jakarta.validation.constraints.NotNull;
import org.hibernate.validator.constraints.NotBlank;


public record Book(

        @NotNull
        Integer bookId,

        @NotBlank
        String bookName,

        @NotBlank
        String bookAuthor

) {
}
