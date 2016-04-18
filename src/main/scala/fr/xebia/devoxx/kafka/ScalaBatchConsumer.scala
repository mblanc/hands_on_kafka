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
    // TODO 3_1
    ???
  }

  private def assignPartitions(consumer: KafkaConsumer[String, String]) {
    // TODO 3_2
    ???
  }

  private def seek(consumer: KafkaConsumer[String, String]) {
    // TODO 3_3
    ???
  }

  private def process(consumer: KafkaConsumer[String, String]) {
    // TODO 3_4
    ???
  }

}
