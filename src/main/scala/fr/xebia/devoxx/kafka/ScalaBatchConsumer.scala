package fr.xebia.devoxx.kafka

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

object ScalaBatchConsumer {

  def main(args: Array[String]) {
    val consumer = createKafkaConsumer()

    assignPartitions(consumer)

    seek(consumer)

    process(consumer)
  }

  private def createKafkaConsumer(): KafkaConsumer[String, String] = {
    import scala.collection.JavaConversions._
    val props = Map(
      "bootstrap.servers" -> "localhost:9092,localhost:9093",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "enable.auto.commit" -> "false",
      "group.id" -> "batch"
    )
    new KafkaConsumer[String, String](props)
  }

  private def assignPartitions(consumer: KafkaConsumer[String, String]) {
    import scala.collection.JavaConversions._
    val partitionInfos = consumer.partitionsFor("devoxx")
    val topicPartitions = partitionInfos.map(partitionInfo => new TopicPartition("devoxx", partitionInfo.partition()))
    System.out.println(topicPartitions)
    consumer.assign(topicPartitions)
  }

  private def seek(consumer: KafkaConsumer[String, String]) {
    consumer.seekToBeginning()
  }

  private def process(consumer: KafkaConsumer[String, String]) {
    import scala.collection.JavaConversions._
    var records = consumer.poll(1000)
    while (!records.isEmpty) {
      for (record <- records) {
        println(s"topic = ${record.topic}, partition: ${record.partition}, offset: ${record.offset}: ${record.value}")
      }
      records = consumer.poll(1000)
    }
  }

}
