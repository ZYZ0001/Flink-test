package com.atguigu.flink.api

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._

object UDF {

    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val wordDataStream: DataStream[String] = env.readTextFile("input/wc.log").flatMap(_.split(" "))

        wordDataStream.filter(new myFilter).print()
//        wordDataStream.filter(s => !"hello".equalsIgnoreCase(s.trim)).print()

        env.execute("udf_test")
    }
}

class myFilter extends FilterFunction[String] {
    override def filter(value: String): Boolean = !"hello".equalsIgnoreCase(value.trim)
}
