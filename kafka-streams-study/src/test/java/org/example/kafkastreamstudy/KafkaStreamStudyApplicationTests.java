package org.example.kafkastreamstudy;

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

@SpringBootTest
class KafkaStreamStudyApplicationTests {

    @Autowired
    StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @Test
    void testWordCount() {
        Topology topology = streamsBuilderFactoryBean.getTopology();
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"test");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"mock:1234");
        // 기본 Serde 설정 추가
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        TopologyTestDriver testDriver = new TopologyTestDriver(topology,properties);
        TestInputTopic<String,String> inputTopic = testDriver.createInputTopic("input-topic",new StringSerializer(),new StringSerializer());
        TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic("output-topic", new StringDeserializer(), new StringDeserializer());

        inputTopic.pipeInput("hello world hello");
        outputTopic.readKeyValuesToList().forEach(System.out::println);


    }

    @Test
    void contextLoads() {


    }

}
