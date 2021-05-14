package com.learnkafka.producer;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
public class LibraryEventProducerUnitTest {


    @Mock
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Spy
    ObjectMapper objectMapper = new ObjectMapper();

    @InjectMocks
    LibraryEventProducer libraryEventProducer;

    @Test
    void sendLibraryEventAsync_failure() throws JsonProcessingException {

        //given
        Book book = Book.builder()
                .bookId(22)
                .bookAuthor("Dilendra")
                .bookName("SpringBoot with kafka")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();

        SettableListenableFuture future = new SettableListenableFuture();
        future.setException(new Exception("Exception calling Kafka"));
        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);
        // when
        assertThrows(Exception.class, ()-> libraryEventProducer.sendLibraryEventAsync(libraryEvent).get());

    }

    @Test
    void sendLibraryEventAsync_success() throws JsonProcessingException, ExecutionException, InterruptedException {

        //given
        Book book = Book.builder()
                .bookId(22)
                .bookAuthor("Dilendra")
                .bookName("SpringBoot with kafka")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();
        String record = objectMapper.writeValueAsString(libraryEvent);

        SettableListenableFuture future = new SettableListenableFuture();
        String topic = "library-events";
        ProducerRecord<Integer, String> producerRecord = new ProducerRecord(topic, libraryEvent.getLibraryEventId(), record);


        //TopicPartition topicPartition, long baseOffset, long relativeOffset, long timestamp,
        //                          Long checksum, int serializedKeySize, int serializedValueSize
        RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition(topic, 1), 1, 1, 342, System.currentTimeMillis(), 1, 2);
        SendResult<Integer, String> sendResult = new SendResult<Integer, String>(producerRecord, recordMetadata);
        future.set(sendResult);
        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);
        // when
       ListenableFuture<SendResult<Integer, String>> listenableFuture = libraryEventProducer.sendLibraryEventAsync(libraryEvent);

       // then
       SendResult<Integer, String> result = listenableFuture.get();
       assert result.getRecordMetadata().partition() == 1;

    }
}
