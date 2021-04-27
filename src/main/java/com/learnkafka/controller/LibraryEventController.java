package com.learnkafka.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.producer.LibraryEventProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.json.JsonParseException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@RestController
@Slf4j
public class LibraryEventController {

    @Autowired
    LibraryEventProducer libraryEventProducer;
    @PostMapping("/v1/libraryevent")
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
        log.info("Before sendLibraryEvents");
        //{"libraryEventId":null,"book":{"bookId":456,"bookName":"Kafka Using Spring Boot","bookAuthor":"Dilip"}}
        //curl -i \
        //-d '{"libraryEventId":null,"book":{"bookId":456,"bookName":"Kafka Using Spring Boot","bookAuthor":"Dilip"}}' \
        //-H "Content-Type: application/json" \
        //-X POST http://localhost:8080/v1/libraryevent
       // libraryEventProducer.sendLibraryEvent(libraryEvent);
        //SendResult<Integer, String> sendResult = libraryEventProducer.sendLibraryEventSynchronus(libraryEvent);
        libraryEventProducer.sendLibraryEventAsync(libraryEvent);
        //log.info("Before sendLibraryEvents : {}", sendResult.toString());
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

}
