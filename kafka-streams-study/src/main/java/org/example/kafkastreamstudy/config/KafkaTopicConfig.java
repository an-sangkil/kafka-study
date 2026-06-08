package org.example.kafkastreamstudy.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Description
 *
 * @author yohan.an
 * @version Copyright (C) 2025 by KakaoHealthcare. All right reserved.
 * @since 2025. 12. 10.
 */
@Configuration
public class KafkaTopicConfig {

    /**
     * 카프카스트림즈가 구독하는 소스토픽을 미리 생성 합니다.
     *
     */
    @Bean
    public NewTopic inputTopic() {
        return TopicBuilder
                .name("input-topic")
                .partitions(1)
                .replicas(1)
                .build();

    }
    @Bean
    public NewTopic outputTopic() {
        return TopicBuilder
                .name("output-topic")
                .partitions(1)
                .replicas(1)
                .build();
    }
    @Bean
    public NewTopic inputWordCountTopic() {
        return TopicBuilder
                .name("input-wordcount-topic")
                .partitions(1)
                .replicas(1)
                .build();
    }
    @Bean
    public NewTopic outputWordCountTopic() {
        return TopicBuilder
                .name("output-wordcount-topic")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic inputWordCountWindowTopic(){
        return TopicBuilder.name("input-wordcount-window-topic").partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic outputWordCountWindowTopic(){
        return TopicBuilder.name("output-wordcount-window-topic").partitions(1).replicas(1).build();
    }
}
