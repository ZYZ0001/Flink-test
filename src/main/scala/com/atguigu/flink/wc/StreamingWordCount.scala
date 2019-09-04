package com.atguigu.flink.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object StreamingWordCount {

    def main(args: Array[String]): Unit = {
        val params: ParameterTool = ParameterTool.fromArgs(args)
        val host: String = params.get("host")
        val port: Int = params.getInt("port")
        val path: String = params.get("outputdir")

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//        env.setParallelism(2)
//        env.disableOperatorChaining() //取消全局合并任务

        val inputDataStream: DataStream[String] = env.socketTextStream(host, port)

        val wordCountDataStream: DataStream[(String, Int)] = inputDataStream.flatMap(_.split("\\s"))
//            .filter(_.nonEmpty).setParallelism(2)
//            .filter(_.nonEmpty).disableChaining() //filter单独为一个任务, 不参与合并
            .filter(_.nonEmpty).startNewChain() //从前面切开任务合并
            .map((_, 1)).setParallelism(4)
            .keyBy(0)
            .sum(1)
        wordCountDataStream.print().setParallelism(1)
//        wordCountDataStream.writeAsText(path)

        env.execute()
    }
}
