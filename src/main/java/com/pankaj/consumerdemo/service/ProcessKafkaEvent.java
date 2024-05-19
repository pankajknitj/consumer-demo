package com.pankaj.consumerdemo.service;

import com.pankaj.consumerdemo.model.LibraryEvent;
import com.pankaj.consumerdemo.model.LibraryEventType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.stereotype.Service;
/** Service layer of kafka consumer*/
@Slf4j
@Service
public class ProcessKafkaEvent {
    /* bussiness logic should be there*/
    public void process(LibraryEvent event){
        /* Further processing is not possible*/
        if(event.libraryEventType().equals(LibraryEventType.UPDATE) && event.libraryEventId()==null){
            throw new IllegalArgumentException("Library-event-id is required to make update");
        }
        /* Just for testing purpose, if it send event to recover topic*/
        else if(event.libraryEventId() == 999){
            throw new RecoverableDataAccessException("jjust for testing");
        }
        else{
            log.info("Processing successfully completed !!!");
        }
    }
}
