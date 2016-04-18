package fr.xebia.devoxx.kafka;

import org.apache.commons.lang.NotImplementedException;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class JavaBatchConsumer {

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        assignPartitions(consumer);

        seek(consumer);

        process(consumer);
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        // TODO 3_1
        throw new NotImplementedException();
    }

    private static void assignPartitions(KafkaConsumer<String, String> consumer) {
        // TODO 3_2
        throw new NotImplementedException();
    }

    private static void seek(KafkaConsumer<String, String> consumer) {
        // TODO 3_3
        throw new NotImplementedException();
    }

    private static void process(KafkaConsumer<String, String> consumer) {
        // TODO 3_4
        throw new NotImplementedException();
    }


}
