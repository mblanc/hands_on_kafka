package fr.xebia.devoxx.kafka;

public class JavaKStream {

    public static void main(String[] args) throws Exception {
        System.out.println("GO");

        // TODO 5_1 : Create a KStreamBuilder

        // TODO 5_2 : Create a source KStream : the stream of messages from topic devoxx connect

        // TODO 5_3 : Create a new sink KStream from the source KStream with the map method : send new KeyValue message, prepend "STREAM : " to the value of the message

        // TODO 5_4 : Send the message from the sink KStream to the Kafka topic devoxx-streams-out

        // TODO 5_5 : Create a KafkaStreams object from this KStreamBuilder and a Properties object

        // TODO 5_6 : start the KafkaStreams
    }
}
