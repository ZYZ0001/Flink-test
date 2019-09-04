package com.atguigu.flink.api

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.util.Random

/**
  * 自定义输入端
  */
object SourceByMySource {

    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        val mySourceDataStream: DataStream[String] = env.addSource(new MySource())

        mySourceDataStream.print("-->")
        env.execute()
    }
}

class MySource extends SourceFunction[String] {

    // 定义一个flag, 表示数据源是否正常运行
    var flag = true

    override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {
        while (flag) {
            val out = System.currentTimeMillis() + ":" + new Random().nextGaussian()

            sourceContext.collect(out)
            Thread.sleep(500)
        }
    }

    override def cancel(): Unit = {
        flag = false
    }
}
