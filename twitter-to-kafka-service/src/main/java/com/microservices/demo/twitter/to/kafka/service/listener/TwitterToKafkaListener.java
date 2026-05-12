package com.microservices.demo.twitter.to.kafka.service.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.StatusAdapter;

@Component
public class TwitterToKafkaListener extends StatusAdapter {
    private static final Logger LOGGER= LoggerFactory.getLogger(TwitterToKafkaListener.class);
    @Override
    public void onStatus(Status status) {
        LOGGER.info("Twitter status with text {}",status.getText());
    }
}
