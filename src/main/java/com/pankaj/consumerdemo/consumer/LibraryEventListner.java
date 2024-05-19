package com.pankaj.consumerdemo.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pankaj.consumerdemo.model.LibraryEvent;
import com.pankaj.consumerdemo.service.ProcessKafkaEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Kafka listener with default ack*/
@Component
@Slf4j
@RequiredArgsConstructor
public class LibraryEventListner {

    private final ProcessKafkaEvent service;
    private final ObjectMapper mapper;

    /* A kafka Listener*/
    @KafkaListener(topics = {"library-events"})
    public void onMessage(ConsumerRecord<Integer,String> record) throws JsonProcessingException {
        LibraryEvent event = mapper.readValue(record.value(), LibraryEvent.class);
        service.process(event);
    }
}
