package org.example.kafkastreamstudy.topolgy;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Description
 *
 * @author yohan.an
 * @version Copyright (C) 2025 by KakaoHealthcare. All right reserved.
 * @since 2025. 12. 10.
 */
@SpringBootTest
class WordCountTopologyTest {

    @Autowired
    StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @Test
    void wordCountTopology() {

        Topology topology = streamsBuilderFactoryBean.getTopology();
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"test");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"mock:1234");
        // 기본 Serde 설정 추가
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        TopologyTestDriver testDriver = new TopologyTestDriver(topology,properties);
        TestInputTopic<String,String> inputTopic = testDriver.createInputTopic("input-wordcount-topic", Serdes.String().serializer() ,Serdes.String().serializer());
        TestOutputTopic<String, Long> outputTopic = testDriver.createOutputTopic("output-wordcount-topic", Serdes.String().deserializer(), Serdes.Long().deserializer());

        inputTopic.pipeInput("hello world hello world");
        inputTopic.pipeInput("hello world");
        inputTopic.pipeInput("kafka streams");

        outputTopic.readKeyValuesToList().forEach(System.out::println);


    }

}