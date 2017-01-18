package fr.xebia.devoxx.kafka;

import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;

public class WordCountJob {

    public static void main(String[] args) throws Exception {
        KStreamBuilder builder = new KStreamBuilder();

        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        KStream<String, String> source = builder.stream("streams-file-input");

        KTable<String, Long> wordCounts = source
                .flatMapValues(value -> {
                    System.out.println(value);
                    return Arrays.asList(value.toLowerCase().split("\\W+"));
                })
                .groupByKey()
                .count("Counts");

        wordCounts.toStream().to(stringSerde, longSerde, "streams-wordcount-output");


        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "devoxx-wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();
        System.out.println("START");
    }
}