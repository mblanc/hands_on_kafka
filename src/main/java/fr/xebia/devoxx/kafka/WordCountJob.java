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

        final Serializer<String> stringSerializer = new StringSerializer();
        final Deserializer<String> stringDeserializer = new StringDeserializer();
        final Serializer<Long> longSerializer = new LongSerializer();
        final Deserializer<Long> longDeserializer = new LongDeserializer();

        KStream<String, String> source = builder.stream("devoxx-wordcount");

        KTable<String, Long> wordCounts = source
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                .map((key, value) -> new KeyValue<>(value, value))
                .countByKey(stringSerializer, longSerializer, stringDeserializer, longDeserializer, "Counts");

        wordCounts.to("devoxx-wordcount-out");


        Properties props = new Properties();
        props.put(StreamsConfig.JOB_ID_CONFIG, "devoxx-wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        props.put(StreamsConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(StreamsConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        props.put(StreamsConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(StreamsConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();
        System.out.println("START");
    }
}