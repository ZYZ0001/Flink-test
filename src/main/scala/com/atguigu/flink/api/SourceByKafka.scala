package com.atguigu.flink.api

import java.io.InputStreamReader
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

object SourceByKafka {

    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        val properties = new Properties()
        properties.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream("kafka.properties")))

        val kafkaDataStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("kafka_flink", new SimpleStringSchema(), properties))

        //        kafkaDataStream.print("kafka: ")
        val wordCountDataStream: DataStream[(String, Int)] = kafkaDataStream.flatMap(_.split(" "))
            .filter(_.nonEmpty)
            .map((_, 1))
            .keyBy(0)
            .reduce((x, y) => (x._1, x._2))
        wordCountDataStream.print().setParallelism(1)

        env.execute()
    }
}
