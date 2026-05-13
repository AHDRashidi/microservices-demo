package com.microservices.demo.twitter.to.kafka.service.runner.impl;

import com.microservices.demo.config.TwitterToKafkaServiceConfigData;
import com.microservices.demo.twitter.to.kafka.service.exception.TwitterToKafkaServiceException;
import com.microservices.demo.twitter.to.kafka.service.listener.TwitterToKafkaListener;
import com.microservices.demo.twitter.to.kafka.service.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

@Component
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets",havingValue = "true")
public class MockKafkaStreamRunner implements StreamRunner {
    private static final Logger LOG= LoggerFactory.getLogger(MockKafkaStreamRunner.class);
    private final TwitterToKafkaServiceConfigData configData;
    private final TwitterToKafkaListener listener;
    private static final Random RANDOM=new Random();
    private static final String[] WORDS=new String[] {
            "Lorem","ipsum","dolor","sit","amet","elit","massa","sed","purus","lectus","funny"
            ,"salad","mimick","hive","spark","madona","july","september","october","fine","fin","total"
    };
    private static final String tweetAsRawJson= """
            {
            "created_at":"{0}",
            "id":"{1}",
            "text":"{2}",
            "user":{"id":"{3}"}
            }""";

    private static String TWITTER_STATUS_DATE_FORMAT = "EEE MMM dd HH:mm:ss zzz yyy";

    public MockKafkaStreamRunner(TwitterToKafkaServiceConfigData configData,
                                 TwitterToKafkaListener listener) {
        this.configData = configData;
        this.listener = listener;
    }

    @Override
    public void start() throws TwitterException {
        String[] keyWords=configData.getTwitterKeywords().toArray(new String[0]);
        int minTweetLengt=configData.getMockMinTweetLength();
        int maxTweetLenght=configData.getMockMaxTweetLength();
        long sleepTimeMs=configData.getMockSleepMs();
        LOG.info("Starting Mock filtering twitter stream for keywords {}",keyWords);
        simulateTweeterStream(keyWords, minTweetLengt, maxTweetLenght, sleepTimeMs);
    }

    private void simulateTweeterStream(String[] keyWords, int minTweetLengt,
                                       int maxTweetLenght, long sleepTimeMs) {
        Executors.newSingleThreadExecutor().submit(() -> {
                    try {


                        while (true) {
                            String formattedTweetAsTawJson = getFormattedTweet(keyWords, minTweetLengt, maxTweetLenght);
                            Status status = TwitterObjectFactory.createStatus(formattedTweetAsTawJson);
                            listener.onStatus(status);
                            sleep(sleepTimeMs);
                        }
                    }catch (TwitterException ex){
                        LOG.error("Error creating twitter status",ex);
                    }
                }
        );
    }

    private void sleep(long sleepTimeMs) {
        try{
            Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
            throw new TwitterToKafkaServiceException("Error while sleeping for waiting new status to be created!");
        }
    }

    private String getFormattedTweet(String[] keyWords, int minTweetLengt, int maxTweetLenght) {
        String[] params = new String[]{
                ZonedDateTime.now().format(DateTimeFormatter.ofPattern(TWITTER_STATUS_DATE_FORMAT, Locale.ENGLISH)),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
                getRandomTweetContent(keyWords, minTweetLengt, maxTweetLenght),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE))
        };
        return formatTweetAsJsonWithParams(params);

    }

    private String formatTweetAsJsonWithParams(String[] params) {
        String tweet = tweetAsRawJson;
        for(int i = 0; i< params.length; i++){
            tweet=tweet.replace("{"+i+"}", params[i]);
        }
        return tweet;
    }

    private String getRandomTweetContent(String[] keyWords, int minTweetLengt, int maxTweetLenght) {
        StringBuilder tweet=new StringBuilder();
        int tweetLenght=RANDOM.nextInt(maxTweetLenght-minTweetLengt+1)+minTweetLengt;
        return constructRandomTweet(keyWords, tweet, tweetLenght);
    }

    private String constructRandomTweet(String[] keyWords, StringBuilder tweet, int tweetLenght) {
        for(int i = 0; i< tweetLenght; i++){
            tweet.append(WORDS[RANDOM.nextInt(WORDS.length)]).append(" ");
            if(i== tweetLenght /2){
                tweet.append(keyWords[RANDOM.nextInt(keyWords.length)]).append(" ");
            }
        }
        return tweet.toString();
    }
}
