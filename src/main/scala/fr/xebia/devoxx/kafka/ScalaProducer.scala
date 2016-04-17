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
      val message: String = produceData()
      val record: ProducerRecord[String, String] = new ProducerRecord[String, String]("devoxx", message)
      sendAsynchronously(producer, record)
      Thread.sleep(1000)
    }
  }

  private def createKafkaProducer(): KafkaProducer[String, String] = {
    import scala.collection.JavaConversions._
    val props = Map(
      "bootstrap.servers" -> "localhost:9092,localhost:9093",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",

      // optional properties
      // 0 : will not wait for any reply from the broker before assuming the message was sent successfully
      // 1 : will receive a success response from the broker the moment the leader replica received the message
      // -1 or all : the Producer will receive a success response from the broker once all in-sync replicas received the message
      "acks" -> "1",

      // the amount of memory the producer will use to buffer messages waiting to be sent to brokers
      "buffer.memory" -> "33554432",

      // none or snappy or gzip
      "compression.type" -> "none",

      // When multiple records are sent to the same partition, the producer will batch them together.
      // This parameter controls the amount of memory in bytes (not messages!) that will be used for each batch
      // When the batch is full, all the messages in the batch will be sent
      "batch.size" -> "16384",

      // control the amount of time we wait for additional messages before sending the current batch
      "linger.ms" -> "0",

      // be able to track the source of requests beyond just ip/port by allowing
      // a logical application name to be included in server-side request logging
      "client.id" -> "",

      // how many messages the producer will send to the server without receiving responses
      //  Setting this to 1 will guarantee that messages will be written to the broker in the order they were sent,
      // even when retries occure
      "max.in.flight.requests.per.connection" -> "5",

      // how long (ms) the producer will wait for reply from the server when sending data
      "timeout.ms" -> "30000",

      // how long (ms) the producer will wait for reply from the server when requesting metadata
      // such as who are the current leaders for the partitions we are writing to
      "metadata.fetch.timeout.ms" -> "60000"
    )

    new KafkaProducer[String, String](props)
  }

  private def produceData(): String = {
    val now: Date = Calendar.getInstance.getTime
    val averageSystemLoad: Double = ManagementFactory.getOperatingSystemMXBean.getSystemLoadAverage
    s"$now avg_load : $averageSystemLoad"
  }

  private def fireAndForget(producer: KafkaProducer[String, String], record: ProducerRecord[String, String]) {
    try {
      producer.send(record)
    } catch {
      case e: Exception =>
        // only catch exception before sending message
        e.printStackTrace()
    }
  }

  private def sendSynchronously(producer: KafkaProducer[String, String], record: ProducerRecord[String, String]) {
    try {
      producer.send(record).get
    } catch {
      case e: Exception =>
        // catch all exceptions
        e.printStackTrace()
    }
  }

  private def sendAsynchronously(producer: KafkaProducer[String, String], record: ProducerRecord[String, String]) {
    producer.send(record, new DemoProducerCallback())
  }

}

class DemoProducerCallback extends Callback {
  def onCompletion(recordMetadata: RecordMetadata, e: Exception) {
    if (e != null) {
      e.printStackTrace()
    } else if (recordMetadata != null) {
      println(s"message ${recordMetadata.offset} sent to partition ${recordMetadata.partition} of topic ${recordMetadata.topic}")
    }
  }
}
