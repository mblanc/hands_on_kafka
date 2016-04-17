package fr.xebia.devoxx.kafka

import java.util.Properties

import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.streams.kstream.{KeyValueMapper, KStream, KStreamBuilder}
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig}

object ScalaKStream {

  def main(args: Array[String]) {
    System.out.println("GO")

    // TODO 5_1 : Create a KStreamBuilder
    val kStreamBuilder: KStreamBuilder = new KStreamBuilder

    // TODO 5_2 : Create a source KStream : the stream of messages from topic devoxx connect
    val source: KStream[String, String] = kStreamBuilder.stream("devoxx-connect")

    // TODO 5_3 : Create a new sink KStream from the source KStream with the map method : send new KeyValue message, prepend "STREAM : " to the value of the message
    val sink: KStream[String, String] = source.map {
      new KeyValueMapper[String, String, KeyValue[String, String]] {
        override def apply(key: String, value: String): KeyValue[String, String] = new KeyValue[String, String](key, "STREAM : " + value)
      }
    }

    // TODO 5_4 : Send the message from the sink KStream to the Kafka topic devoxx-streams-out
    sink.to("devoxx-streams-out")

    // TODO 5_5 : Create a KafkaStreams object from this KStreamBuilder and a Properties object
    val props: Properties = new Properties
    props.put(StreamsConfig.JOB_ID_CONFIG, "streams")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093")
    props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181")
    props.put(StreamsConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(StreamsConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(StreamsConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    props.put(StreamsConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])

    // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
    props.put(StreamsConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

    val kafkaStreams: KafkaStreams = new KafkaStreams(kStreamBuilder, props)

    // TODO 5_6 : start the KafkaStreams
    kafkaStreams.start()
  }

}
