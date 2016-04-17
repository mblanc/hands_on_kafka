package fr.xebia.devoxx.kafka;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JavaSparkStreaming {

    public static void main(String[] args) {
        JavaStreamingContext context = createStreamContext();
        JavaPairReceiverInputDStream<String, String> stream = createStream(context);

        // TODO Step 6_3
        stream.map(v1 -> extractValueFromRecord(v1._2()))
            .foreachRDD(JavaSparkStreaming::displayAvg);

        context.start();
        context.awaitTermination();
    }

    public static JavaStreamingContext createStreamContext() {
        // TODO Step 6_1
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("streaming-client");

        return new JavaStreamingContext(conf, Durations.seconds(5));
    }

    public static JavaPairReceiverInputDStream<String, String> createStream(JavaStreamingContext context) {
        // TODO Step 6_2
        Map<String, Integer> topics = new HashMap<>();
        topics.put("devoxx", 4);

        // there are several way to connect spark to kafka see http://spark.apache.org/docs/latest/streaming-kafka-integration.html
        // we cannot use createDirectStream see https://issues.apache.org/jira/browse/SPARK-12177
        return KafkaUtils.createStream(context, "localhost:2181", "streaming-client", topics);
    }

    public static double extractValueFromRecord(String line) {
        final Pattern pattern = Pattern.compile(".{24}: avg_load: (.*)");
        final Matcher matcher = pattern.matcher(line);
        if(matcher.find()) {
            return Double.valueOf(matcher.group(1));
        }
        return 0;
    }

    public static void displayAvg(JavaRDD<Double> rdd) {
        Double sum = rdd.fold(0d, (v1, v2) -> v1 + v2);
        long count = rdd.count();

        Double avg = count == 0 ? 0 : sum/count;

        SimpleDateFormat timeFormatter = new SimpleDateFormat("HH:mm:ss");

        System.out.println(timeFormatter.format(new Date()) + " : last 5 seconds average load => " + avg);
    }
}
