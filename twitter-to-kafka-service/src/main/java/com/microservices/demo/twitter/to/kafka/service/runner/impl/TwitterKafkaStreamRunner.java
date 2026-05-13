package com.microservices.demo.twitter.to.kafka.service.runner.impl;

import com.microservices.demo.config.TwitterToKafkaServiceConfigData;
import com.microservices.demo.twitter.to.kafka.service.listener.TwitterToKafkaListener;
import com.microservices.demo.twitter.to.kafka.service.runner.StreamRunner;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.FilterQuery;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

@Component
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets",havingValue = "false",matchIfMissing = true)
public class TwitterKafkaStreamRunner implements StreamRunner {
    private static final Logger LOGGER= LoggerFactory.getLogger(TwitterKafkaStreamRunner.class);
    private final TwitterToKafkaServiceConfigData config;
    private final TwitterToKafkaListener listener;
    private TwitterStream twitterStream;

    public TwitterKafkaStreamRunner(TwitterToKafkaServiceConfigData config, TwitterToKafkaListener listener) {
        this.config = config;
        this.listener = listener;
    }

    @Override
    public void start() throws TwitterException {
        twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(listener);
        addFilter();

    }

    @PreDestroy
    public void shutDown(){
        if(twitterStream!=null){
            LOGGER.info("Shutting down twitter stream...");
            twitterStream.shutdown();
        }
    }

    private void addFilter() {
        String[] keywords=config.getTwitterKeywords().toArray(new String[0]);
        FilterQuery filterQuery=new FilterQuery(keywords);
        twitterStream.filter(filterQuery);
        LOGGER.info("Started filtering twitter stream for keywords {}",keywords);
    }
}

