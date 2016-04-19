package fr.xebia.devoxx.kafka;


//import org.apache.commons.lang.NotImplementedException;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//
//import java.text.SimpleDateFormat;
//import java.util.Date;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;

public class JavaSparkStreaming {

//    public static void main(String[] args) {
//        JavaStreamingContext context = createStreamContext();
//        JavaPairReceiverInputDStream<String, String> stream = createStream(context);
//
//        // TODO Step 6_3
//
//        context.start();
//        context.awaitTermination();
//    }
//
//    public static JavaStreamingContext createStreamContext() {
//        // TODO Step 6_1
//        throw new NotImplementedException();
//    }
//
//    public static JavaPairReceiverInputDStream<String, String> createStream(JavaStreamingContext context) {
//        // TODO Step 6_2
//        throw new NotImplementedException();
//    }
//
//    public static double extractValueFromRecord(String line) {
//        final Pattern pattern = Pattern.compile(".{24}: avg_load: (.*)");
//        final Matcher matcher = pattern.matcher(line);
//        if(matcher.find()) {
//            return Double.valueOf(matcher.group(1));
//        }
//        return 0;
//    }
//
//    public static void displayAvg(JavaRDD<Double> rdd) {
//        Double sum = rdd.fold(0d, (v1, v2) -> v1 + v2);
//        long count = rdd.count();
//
//        Double avg = count == 0 ? 0 : sum/count;
//
//        SimpleDateFormat timeFormatter = new SimpleDateFormat("HH:mm:ss");
//
//        System.out.println(timeFormatter.format(new Date()) + " : last 5 seconds average load => " + avg);
//    }
}
