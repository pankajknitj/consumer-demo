package com.pankaj.consumerdemo.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;
import java.util.Objects;


/** Kafka consumer configuration*/
@Configuration
@EnableKafka
@RequiredArgsConstructor
@Slf4j
public class LibraryEventConsumer {
    /* Dependencies*/
    private final KafkaProperties properties;
    private final KafkaTemplate<Integer, String> kafkaTemplate;

    /** An event recovery
     * Will be invoked if all retry attempts exhausted
     * It will publish the same event with some additional headers to the specified topic and partition
     * publishing activity will be handled by the kafka itself*/
    DeadLetterPublishingRecoverer eventRecoverer(){
        var recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,(r,e)->{
                /* publish to this topic if exception is of type RecoverableDataAccessException
                * else publish to dead-later-topic, specified below*/
                if(e.getCause() instanceof RecoverableDataAccessException){
                    return new TopicPartition("library-event-recover",r.partition());
                }else{
                    return new TopicPartition("library-event-dlt", r.partition());
                }
        });
        return recoverer;
    }

    /** An Error handler to handle failed event in case of exception during processing*/
    private DefaultErrorHandler errorHandler(){

        /* wait for 1 sec before retry and max retry will be 2*/
        FixedBackOff backoff = new FixedBackOff(1000L,2);

        /* exponential backoff*/
        ExponentialBackOff exponentialBackOff = new ExponentialBackOff(1000L,2);
        exponentialBackOff.setMaxAttempts(3);

        /* Initializing error handler
        * First arg is a recoverer, which will be invoked in case all retry attempts exhausted*/
        var errorHandler = new DefaultErrorHandler(
                eventRecoverer(),
               //backoff
                exponentialBackOff
        );

        /* Don't retry if these exception occurs*/
        var exceptionToIgnore = List.of(IllegalArgumentException.class);

        /* adding list to non retryable exception*/
        exceptionToIgnore.forEach(errorHandler::addNotRetryableExceptions);

        /* retry listener, will be invoked for each retry to see whats going on with retry*/
        errorHandler.setRetryListeners((record, ex, deliveryAttempt) -> {
            log.info("error occurred while processing, exception is: {}",ex.getMessage());
        });

        return errorHandler;
    }

    /** Overriding the kafka container to add some extra properties
     * Added error handler to handle event in case to event processing failure*/
    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(ConcurrentKafkaListenerContainerFactoryConfigurer configurer, ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory, ObjectProvider<ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>>> kafkaContainerCustomizer, ObjectProvider<SslBundles> sslBundles) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory();
        configurer.configure(factory, (ConsumerFactory)kafkaConsumerFactory.getIfAvailable(() -> {
            return new DefaultKafkaConsumerFactory(this.properties.buildConsumerProperties((SslBundles)sslBundles.getIfAvailable()));
        }));
        Objects.requireNonNull(factory);
        kafkaContainerCustomizer.ifAvailable(factory::setContainerCustomizer);

        /* Setting ack to manual*/
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        /* Setting error handler
        * This error handler will be invoked if kafka consumer encounter any exception during processing to event*/
        factory.setCommonErrorHandler(errorHandler());
        return factory;
    }

}
