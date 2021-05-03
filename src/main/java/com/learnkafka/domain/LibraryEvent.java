package com.learnkafka.domain;

import lombok.*;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class LibraryEvent {
    private Integer libraryEventId;
    private LibraryEventType libraryEventType;
    @NonNull
    private Book book;
}
