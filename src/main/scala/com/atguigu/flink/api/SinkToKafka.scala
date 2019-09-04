package com.atguigu.flink.api

import java.io.InputStreamReader
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

object SinkToKafka {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        val properties = new Properties()
        properties.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream("kafka.properties")))

        val wordDataStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("flink_test", new SimpleStringSchema(), properties))
            .flatMap(_.split(" "))
            .filter(_.nonEmpty)
            .map(word => {
                Thread.sleep(500)
                ("new: " + word)
            })

        wordDataStream.print()
        wordDataStream.addSink(new FlinkKafkaProducer011[String]("hadoop102:9092", "flink_test", new SimpleStringSchema()))

        env.execute()
    }
}
