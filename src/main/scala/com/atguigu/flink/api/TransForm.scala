package com.atguigu.flink.api

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._

import scala.collection.Seq

object TransForm {

    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val listDataStream: DataStream[Student] = env.fromCollection(List(
            Student("1001", "zhang san", 17, 'F'),
            Student("1002", "li si", 21, 'M'),
            Student("1003", "wang wu", 16, 'F'),
            Student("1004", "zhao liu", 18, 'M'),
            Student("1005", "chen qi", 22, 'F'),
            Student("1006", "sun ba", 20, 'M')
        ))

//         val keyByStream: KeyedStream[Student, Char] = listDataStream.keyBy(_.gender)
//         val keyByStream: KeyedStream[Student, String] = listDataStream.keyBy(_.id)
//        keyByStream.print()

//        listDataStream.keyBy("gender")
//            .sum("age")
//            .map(s => (s.gender, s.age))
//            .print("studentSumAge: ")
//            .setParallelism(1)

        val splitStream: SplitStream[Student] = listDataStream.split(s => if (s.gender == 'M') Seq("男生") else Seq("女生"))

        val m: DataStream[Student] = splitStream.select("男生")
        m.print("男生: ")
        val f: DataStream[Student] = splitStream.select("女生")
        f.print("女生: ")

        env.execute()
    }
}
