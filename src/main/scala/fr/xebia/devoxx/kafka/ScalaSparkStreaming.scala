package fr.xebia.devoxx.kafka

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

object ScalaSparkStreaming {

  def main(args: Array[String]): Unit = {
    val streamingContext = createStreamContext()
    val stream = createStream(streamingContext)

    // TODO Step 6_3
    ???

    streamingContext.start()
    streamingContext.awaitTermination()

  }

  def createStreamContext(): StreamingContext = {
    // TODO Step 6_1
    ???
  }

  def createStream(context: StreamingContext): InputDStream[(String, String)] = {
    // TODO Step 6_2
    ???
  }

  def extractValueFromRecord(line: String): Double = {
    val pattern = ".{24}: avg_load: (.*)".r
    line match {
      case pattern(value) => Try(value.toDouble).getOrElse(0)
      case _ => 0
    }
  }

  def displayAvg(rdd: RDD[Double]) = {
    val sum = rdd.fold(0)(_+_)
    val count = rdd.count()
    val avg = if(count == 0) 0 else sum/count

    val timeFormatter = new SimpleDateFormat("HH:mm:ss")

    println(s"${timeFormatter.format(new Date())} : last 5 seconds average load => $avg")
  }

}
