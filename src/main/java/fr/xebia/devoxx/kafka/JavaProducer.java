package fr.xebia.devoxx.kafka;

import org.apache.commons.lang.NotImplementedException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.lang.management.ManagementFactory;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.ExecutionException;

public class JavaProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // instanciation du KafkaProducer
        KafkaProducer<String, String> producer = createKafkaProducer();

        // écriture
        while (true) {
            // TODO 1_2
            Thread.sleep(1000);
        }
    }

    private static String produceData() {
        Date now = Calendar.getInstance().getTime();
        double averageSystemLoad = ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage();
        String message = String.format("%s: avg_load: %f", now.toString(), averageSystemLoad);
        return message;
    }

    private static KafkaProducer<String, String> createKafkaProducer() {
        // TODO 1_1
        throw new NotImplementedException();
    }

    private static void fireAndForget(KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
        // TODO 1_2
        throw new NotImplementedException();
    }

    private static void sendSynchronously(KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
        // TODO 1_3
        throw new NotImplementedException();
    }

    private static void sendAsynchronously(KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
        // TODO 1_4
        throw new NotImplementedException();
    }

    // TODO 1_4
    private static class DemoProducerCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            throw new NotImplementedException();
        }
    }


}
