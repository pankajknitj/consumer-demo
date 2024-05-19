package com.pankaj.consumerdemo.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

//@Component
@Slf4j
public class LibraryEventListnerManualAck {

    @KafkaListener(topics = {"library-events"})
    public void onMessage(ConsumerRecord<Integer,String> record, Acknowledgment acknowledgment){
        log.info("message: {}",record);
        acknowledgment.acknowledge();
    }
}
