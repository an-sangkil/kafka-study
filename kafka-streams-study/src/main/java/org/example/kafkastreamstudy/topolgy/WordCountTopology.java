package org.example.kafkastreamstudy.topolgy;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Description
 *
 * @author yohan.an
 * @version Copyright (C) 2025 by KakaoHealthcare. All right reserved.
 * @since 2025. 12. 10.
 */
@Configuration
public class WordCountTopology {

    /**
     * 윈도우 캐싱기반이 아니기때문에 누적으로 계속 카운트 된다.
     *
     * 케싱된 KWordCount
     * RocksDB 보관기관은 7일 이며, 자동 생성되는 chnageLog는 7일 보관, 512MB의 세그먼트를 가진다.
     * @param streamsBuilder
     * @return
     */
    @Bean
    public KStream<String, Long> kWordCountStream(StreamsBuilder streamsBuilder) {


        // 자동 생성되는 토픽에 대한 설정 , Kafka Streams 내부 토픽 (changelog, repartition topic)
        Map<String, String> logConfig = new HashMap<>();
        logConfig.put("retention.ms", String.valueOf(7L * 24 * 60 * 60 * 1000)); // 7일 보관
        logConfig.put("segment.bytes", "536870912"); // 512MB 세그먼트

        KStream<String, String> stream = streamsBuilder.stream("input-wordcount-topic");
        KTable<String, Long> wordCounts = stream.flatMapValues(value -> Arrays.asList(value.toUpperCase().split(" ")))
                .groupBy((key, value) -> value)
                //.windowedBy()
                .count(

                        Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("wordcount-store")

                                .withCachingEnabled()               // 케싱
                                .withLoggingEnabled(logConfig)      // changelog 토픽 생성 설정
                                .withRetention(Duration.ofMinutes(7))  // 로컬 Rocks db 보관 기관

                ); // ktable로 상태 저장 (stateful)


        wordCounts.toStream().to("output-wordcount-topic");


    return wordCounts.toStream();
    }
}
