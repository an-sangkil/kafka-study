package org.example.kafkastreamstudy.topolgy;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.Arrays;

/**
 *
 *
 * @author yohan.an
 * @version Copyright (C) 2025 by KakaoHealthcare. All right reserved.
 * @since 2025. 12. 16.
 */
@Configuration
public class WordCountWindowTopology {

    /**
     * Tumbling Window (텀플링 윈도우) 기반
     *  - 5분 크기의 비겹침 윈도우 (NoGrace)
     *
     * @param streamsBuilder
     * @return
     */
    @Bean
    public KStream<Windowed<String>, Long> kWordCountWindowStream(StreamsBuilder streamsBuilder) {

        KStream<String, String> stream   = streamsBuilder.stream("input-wordcount-window-topic");
        KTable<Windowed<String>,Long> wordCount    = stream.flatMapValues((readOnlyKey, value) -> {

            String[] words = value.split(" ");
            for (String word : words) {
                System.out.println(word);
            }

            return Arrays.asList(words);
        }).groupBy((key, value) -> value)
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
                .count(Named.as(""),Materialized.<String, Long>as(
                                Stores.persistentWindowStore(
                                        "wordcount-window-store", // 상태 저장소 이름 RocksDB에 저장되는 이름
                                        Duration.ofMinutes(10),         // 윈도우 스토어에 저장된 윈도우데이터 유지 시간 10분, 시간이 지나면 레코드 삭제됨
                                        Duration.ofMinutes(5),          // 윈도우 저장소 크기로 "windowedBy" 와 같게 잡아야 한다. 불일치할경우 의도와는 다른 데이터로 집계 된다.
                                        false                           // 동일키에대한 누적 여부, false로 사용하여 마지막만 남긴다. rocksdb의 부하를 줄일 수있다.

                                )
                        ).withCachingEnabled()
                        //.withLoggingEnabled()
                )
        ;

        wordCount.toStream().to("output-wordcount-window-topic", Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class, 5), Serdes.Long()));

        return wordCount.toStream();
    }
}
