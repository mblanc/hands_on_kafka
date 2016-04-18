package fr.xebia.devoxx.kafka

import java.lang.management.ManagementFactory
import java.util.{Calendar, Date}

import org.apache.kafka.clients.producer.{RecordMetadata, Callback, KafkaProducer, ProducerRecord}


object ScalaProducer {

  def main(args: Array[String]) {
    // instanciation du KafkaProducer
    val producer = createKafkaProducer()

    // écriture
    while (true) {
      // TODO 1_2
      ???
      Thread.sleep(1000)
    }
  }

  private def createKafkaProducer(): KafkaProducer[String, String] = {
    import scala.collection.JavaConversions._
    // TODO 1_1
    ???
  }

  private def produceData(): String = {
    val now: Date = Calendar.getInstance.getTime
    val averageSystemLoad: Double = ManagementFactory.getOperatingSystemMXBean.getSystemLoadAverage
    s"$now avg_load : $averageSystemLoad"
  }

  private def fireAndForget(producer: KafkaProducer[String, String], record: ProducerRecord[String, String]) {
    // TODO 1_2
    ???
  }

  private def sendSynchronously(producer: KafkaProducer[String, String], record: ProducerRecord[String, String]) {
    // TODO 1_3
    ???
  }

  private def sendAsynchronously(producer: KafkaProducer[String, String], record: ProducerRecord[String, String]) {
    // TODO 1_4
    ???
  }

}

class DemoProducerCallback extends Callback {
  // TODO 1_4
  def onCompletion(recordMetadata: RecordMetadata, e: Exception) {
    ???
  }
}
