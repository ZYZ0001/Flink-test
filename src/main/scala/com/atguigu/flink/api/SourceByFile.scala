package com.atguigu.flink.api

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object SourceByFile {

    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        val fileDataStream: DataStream[String] = env.readTextFile("input/wc.log")
        fileDataStream.print("stream").setParallelism(1)
        env.execute("sourceByFile")
    }
}
