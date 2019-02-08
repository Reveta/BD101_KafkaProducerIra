package com.epam.streaming

import org.apache.kafka.clients.producer.KafkaProducer

import scala.concurrent.Future

object KafkaConf {
  private val props = new java.util.Properties()
  props.put("bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
  props.put("auto.commit.intervals.ms", "1000")
  props.put("client.id", "KafkaProducer")
  props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  private val producer: KafkaProducer[Integer, String] = new KafkaProducer[Integer, String](props)

  def getProducer: KafkaProducer[Integer, String] = {
    return producer
  }
}
