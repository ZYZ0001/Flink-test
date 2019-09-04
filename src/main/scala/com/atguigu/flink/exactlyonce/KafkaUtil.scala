package com.atguigu.flink.exactlyonce

import java.io.InputStreamReader
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.json4s.native.Serialization

/**
  * 向Kafka发送测试数据
  */
object KafkaUtil {

  def main(args: Array[String]): Unit = {
    writeKafka("kafkaExactlyOnce")
  }

  def writeKafka(topic: String): Unit = {
    val pro = new Properties()
    pro.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream("kafkaProducer.properties")))

    val produce = new KafkaProducer[String, String](pro)

    for (i <- 1 to 20) {
      val mySQLData = MySQLExactlyOnceClass(i.toString)
      implicit val formats = org.json4s.DefaultFormats
      val outputData: String = Serialization.write(mySQLData)
      produce.send(new ProducerRecord[String, String](topic, outputData))
      println("发送数据: " + outputData)
      try {
        Thread.sleep(1000)
      } catch {
        case e: Exception => println(e.getMessage)
      }
    }

    produce.flush()
  }
}

case class MySQLExactlyOnceClass(value: String)