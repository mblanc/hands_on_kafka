package fr.xebia.devoxx.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.lang.management.ManagementFactory;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class JavaProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // instanciation du KafkaProducer
        KafkaProducer<String, String> producer = createKafkaProducer();

        // écriture
        while (true) {
            // TODO 1_2
            String message = produceData();
            ProducerRecord<String, String> record = new ProducerRecord<>("devoxx", message);
            sendAsynchronously(producer, record);
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
        Map<String, Object> props = new HashMap<>();

        // required properties
        props.put("bootstrap.servers", "localhost:9092,localhost:9093");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // optional properties

        // TODO 1_5
        // 0 : will not wait for any reply from the broker before assuming the message was sent successfully
        // 1 : will receive a success response from the broker the moment the leader replica received the message
        // -1 or all : the Producer will receive a success response from the broker once all in-sync replicas received the message
        props.put("acks", "1");

        // the amount of memory the producer will use to buffer messages waiting to be sent to brokers
        props.put("buffer.memory", "33554432");

        // none or snappy or gzip
        props.put("compression.type", "none");

        // When multiple records are sent to the same partition, the producer will batch them together.
        // This parameter controls the amount of memory in bytes (not messages!) that will be used for each batch
        // When the batch is full, all the messages in the batch will be sent
        props.put("batch.size", "16384");

        // control the amount of time we wait for additional messages before sending the current batch
        props.put("linger.ms", "0");

        // be able to track the source of requests beyond just ip/port by allowing
        // a logical application name to be included in server-side request logging
        props.put("client.id", "");

        // how many messages the producer will send to the server without receiving responses
        //  Setting this to 1 will guarantee that messages will be written to the broker in the order they were sent,
        // even when retries occure
        props.put("max.in.flight.requests.per.connection", "5");

        // how long (ms) the producer will wait for reply from the server when sending data
        props.put("timeout.ms", "30000");

        // how long (ms) the producer will wait for reply from the server when requesting metadata
        // such as who are the current leaders for the partitions we are writing to
        props.put("metadata.fetch.timeout.ms", "60000");

        return new KafkaProducer<>(props);
    }

    private static void fireAndForget(KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
        // TODO 1_2
        try {
            producer.send(record);
        } catch (Exception e) {
            // only catch exception before sending message
            e.printStackTrace();
        }
    }

    private static void sendSynchronously(KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
        // TODO 1_3
        try {
            producer.send(record).get();
        } catch (Exception e) {
            // catch all exceptions
            e.printStackTrace();
        }
    }

    private static void sendAsynchronously(KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
        // TODO 1_4
        producer.send(record,
                (recordMetadata, e) -> {
                    if (e != null) {
                        e.printStackTrace();
                    } else {
                        System.out.printf("message %d sent to partition %d of topic %s%n",
                                recordMetadata.offset(), recordMetadata.partition(), recordMetadata.topic());
                    }
                });
    }

    // TODO 1_4
    private static class DemoProducerCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                // catch message send
                e.printStackTrace();
            } else if (recordMetadata != null) {
                System.out.printf("message %d sent to partition %d of topic %s%n",
                        recordMetadata.offset(), recordMetadata.partition(), recordMetadata.topic());
            }
        }
    }


}
