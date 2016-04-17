package fr.xebia.devoxx.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JavaBatchConsumer {

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        assignPartitions(consumer);

        seek(consumer);

        process(consumer);
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        // TODO 3_1
        Map<String, Object> props = new HashMap<>();

        props.put("bootstrap.servers", "localhost:9092,localhost:9093");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("enable.auto.commit", "false");
        props.put("group.id", "batch");

        return new KafkaConsumer<>(props);
    }

    private static void assignPartitions(KafkaConsumer<String, String> consumer) {
        // TODO 3_2
        List<PartitionInfo> partitionInfos = consumer.partitionsFor("devoxx");
        List<TopicPartition> topicPartitions =  partitionInfos.stream()
                .map(partitionInfo -> new TopicPartition("devoxx", partitionInfo.partition()))
                .collect(Collectors.toList());

        System.out.println(topicPartitions);
        consumer.assign(topicPartitions);
    }

    private static void seek(KafkaConsumer<String, String> consumer) {
        // TODO 3_3
        consumer.seekToBeginning();
    }



    private static void process(KafkaConsumer<String, String> consumer) {
        // TODO 3_4
        ConsumerRecords<String, String> records = consumer.poll(1000);
        while (!records.isEmpty()) {
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(String.format("topic = %s, partition: %d, offset: %d: %s", record.topic(), record.partition(),
                        record.offset(), record.value()));
            }
            records = consumer.poll(1000);
        }
    }


}
