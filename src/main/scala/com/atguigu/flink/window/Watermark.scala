package com.atguigu.flink.window

import org.apache.flink.streaming.api.scala._

object Watermark {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //使用事件时间
        val inputDataStream: DataStream[String] = env.readTextFile("input/wc.log")

        val winStream = inputDataStream.flatMap(_.split(" "))
            .map((_, 1))
            .keyBy(0)
            .countWindow(4)

        winStream.sum(1).print()
        env.execute()

    }
}
