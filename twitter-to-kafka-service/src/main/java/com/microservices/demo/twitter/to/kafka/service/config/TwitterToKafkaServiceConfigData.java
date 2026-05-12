package com.microservices.demo.twitter.to.kafka.service.config;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
@ConfigurationProperties(prefix = "twitter-to-kafka-service")
@Data
public class TwitterToKafkaServiceConfigData {

    private List<String> twitterKeywords;

   /* public List<String> getTwitterKeywords() {
        return twitterKeywords;
    }*/

     @Value("${welcome-message}")
    private String welcomeMessage;

}
