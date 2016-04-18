package fr.xebia.devoxx.kafka

import java.util
import java.util.Collections

import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition

object ScalaConsumer {

  def main(args: Array[String]) {
    import scala.collection.JavaConversions._
    val consumer: KafkaConsumer[String, String] = createKafkaConsumer()

    // TODO 2_7 : shutdownhook


    try {
      while (true) {
        // TODO 2_2
        ???
      }
    } finally {
      consumer.close()
    }
  }

  private def display(record: ConsumerRecord[String, String]) {
    println(s"topic = ${record.topic}, partition: ${record.partition}, offset: ${record.offset}: ${record.value}")
  }

  private def createKafkaConsumer(): KafkaConsumer[String, String] = {
    import scala.collection.JavaConversions._
    // TODO 2_1
    ???
  }

  private def manualCommit(consumer: KafkaConsumer[String, String]) {
    // TODO 2_4
    ???
  }

  private def manualAsynchronousCommit(consumer: KafkaConsumer[String, String]) {
    // TODO 2_5
    ???
  }

}

// TODO 2_6
class HandleRebalance extends ConsumerRebalanceListener {
  import scala.collection.JavaConversions._
  override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]) {
    ???
  }

  override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]) {
    ???
  }
}


