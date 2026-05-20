package com.nik.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Value("${spring.kafka.topic.employee}")
    private String employeeTopic;

    @Bean
    public NewTopic employeeTopic() {
        return TopicBuilder.name(employeeTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }
}

