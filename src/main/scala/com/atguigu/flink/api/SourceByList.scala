package com.atguigu.flink.api

import org.apache.flink.streaming.api.scala._

object SourceByList {

    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        val inputByListDataStream: DataStream[Student] = env.fromCollection(List(
            Student("1001", "xiaoming", 13, 'F'),
            Student("1002", "xiaochen", 16, 'F'),
            Student("1003", "xiaohuang", 13, 'M'),
            Student("1004", "xiaohong", 15, 'M')
        ))

        inputByListDataStream.print("student: ").setParallelism(1)

        env.execute()
    }
}

// 样例类
case class Student(id: String, name: String, age: Int, gender: Char)
