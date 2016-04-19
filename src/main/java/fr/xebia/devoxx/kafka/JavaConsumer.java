package fr.xebia.devoxx.kafka;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Collection;

public class JavaConsumer {

    public static void main(String[] args) {
        //configuration d'un consumer
        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        // TODO 2_7 : shutdownhook

        // TODO 2_2
        try {
            while (true) {
            }
        } finally {
            consumer.close();
        }
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        // TODO 2_1
        throw new NotImplementedException();
    }


    private static void display(ConsumerRecord<String, String> record) {
        System.out.println(String.format("topic = %s, partition: %d, offset: %d: %s", record.topic(), record.partition(),
                record.offset(), record.value()));
    }

    private static void manualCommit(KafkaConsumer<String, String> consumer) {
        // TODO 2_4
        throw new NotImplementedException();
    }

    private static void manualAsynchronousCommit(KafkaConsumer<String, String> consumer) {
        // TODO 2_5
        throw new NotImplementedException();
    }

    // TODO 2_6
    private static class HandleRebalance implements ConsumerRebalanceListener {
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            throw new NotImplementedException();
        }

        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            throw new NotImplementedException();
        }
    }


}
