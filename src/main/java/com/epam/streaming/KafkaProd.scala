package com.epam.streaming

import org.apache.kafka.clients.producer.ProducerRecord

import scala.io.BufferedSource

object KafkaProd extends App {

  val topic = util.Try(args(0)).getOrElse("StreamingTopic")
  println(s"Connecting to $topic")

  var producer = KafkaConf.getProducer

/*TODO!*/

  val stream: BufferedSource = scala.io.Source.fromFile("src/main/resources/train.csv")

  for (line <- stream.getLines) {
    val record: ProducerRecord[Integer, String] = new ProducerRecord(topic, 1, line)
    producer.send(record)
  }

  producer.close()
}
