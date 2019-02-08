package com.epam.streaming

import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.Future
import scala.io.BufferedSource

object KafkaProd extends App {
  implicit val executor = scala.concurrent.ExecutionContext.global
  val topic = util.Try(args(0)).getOrElse("StreamingTopic")
  println(s"Connecting to $topic")


  val stream: BufferedSource = scala.io.Source.fromFile("src\\main\\resources\\sample.csv")

  private val fLine = stream
    .getLines
    .map { line =>
      Future {

        val producer = KafkaConf.getProducer
        val p: ProducerRecord[Integer, String] = new ProducerRecord(topic, 1, line)
        producer.send(p)
      }
    }


  Thread.sleep(60000)

}
