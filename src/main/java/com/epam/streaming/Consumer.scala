package com.epam.streaming


import java.util.concurrent._
import java.util.{Collections, Properties}

import com.sun.javafx.util.Logging
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

import scala.collection.JavaConversions._

class Consumer(val brokers: String,
               val groupId: String,
               val topic: String) extends Logging {

  val props: Properties = createConsumerConfig(brokers, groupId)
  val consumer = new KafkaConsumer[String, String](props)
  var executor: ExecutorService = null



  def shutdown(): Unit = {
    if (consumer != null)
      consumer.close()
    if (executor != null)
      executor.shutdown()
  }

  def createConsumerConfig(brokers: String, groupId: String): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "sandbox-hdp.hortonworks.com:6667")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-1")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props
  }

  def run(): Unit = {
    consumer.subscribe(Collections.singletonList(this.topic))

    Executors.newSingleThreadExecutor.execute(new Runnable {
      override def run(): Unit = {
        while (true) {
          val records: ConsumerRecords[String, String] = consumer.poll(1000)

          for (record <- records) {
            System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset())
          }
        }
      }
    })
  }
}

object Consumer extends App{
  var csvPath ="hdfs://sandbox-hdp.hortonworks.com:8020/homework/streaming"
  val spark = SparkSession.builder().appName("Streaming").master("local[*]").getOrCreate()
  val streamingContext = StreamingContext
  val newArgs = Array("localhost:9092", "2","test")
  val example = new Consumer(newArgs(0), newArgs(1), newArgs(2))
  example.run()
}