package com.atguigu.flink.wc

import org.apache.flink.api.scala._

object WordCount {

    def main(args: Array[String]): Unit = {
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

        val inputDataSet: DataSet[String] = env.readTextFile("input/wc.log")

        val outputDataSet: AggregateDataSet[(String, Int)] = inputDataSet.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)

        outputDataSet.print()
    }
}
