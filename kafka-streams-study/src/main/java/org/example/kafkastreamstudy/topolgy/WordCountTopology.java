package org.example.kafkastreamstudy.topolgy;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Description
 *
 * @author yohan.an
 * @version Copyright (C) 2025 by KakaoHealthcare. All right reserved.
 * @since 2025. 12. 10.
 */
@Configuration
public class WordCountTopology {

    @Bean
    public KStream<String, Long> kWordCountStream(StreamsBuilder streamsBuilder) {

        KStream<String, String> stream = streamsBuilder.stream("input-wordcount-topic");
        KTable<String, Long> wordCounts = stream.flatMapValues(value -> Arrays.asList(value.toUpperCase().split(" ")))
                .groupBy((key, value) -> value)
                .count(

                        Materialized
                                .as("wordcount-store")

                ); // ktable로 상태 저장 (stateful)


        wordCounts.toStream().to("output-wordcount-topic");


    return wordCounts.toStream();
    }
}
