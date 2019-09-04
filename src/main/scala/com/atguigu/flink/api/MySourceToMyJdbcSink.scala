package com.atguigu.flink.api

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.util.Random

object MySourceToMyJdbcSink {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val sourceDataStream: DataStream[Student] = env.addSource(new MySouce1).filter(_ != null)
        sourceDataStream.addSink(new MySink1)
        sourceDataStream.print("student")

        env.execute()
    }
}
// 自定义输入
class MySouce1 extends SourceFunction[Student] {
    var running = true

    override def run(ctx: SourceFunction.SourceContext[Student]): Unit = {
        val random = new Random()
        val list = 1 to 10

        while (running) {
            list.foreach(i => {
                val id = i + 1000
                val name = "wang" + i
                val age = ((random.nextGaussian() + 3) * 20).toInt
                val gender = if (i % 2 == 1) 'M' else 'F'
                ctx.collect(Student(id.toString, name, age, gender))
            })

            Thread.sleep(2000)
        }
    }

    override def cancel(): Unit = running = false
}
// 自定义输出
class MySink1 extends RichSinkFunction[Student] {
    var con: Connection = _
    var update: PreparedStatement = _
    var insert: PreparedStatement = _

    override def open(parameters: Configuration): Unit = {
        println("opening...")
        con = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "password")
        update = con.prepareStatement("update student set age = ? where id = ?")
        insert = con.prepareStatement("insert into student(id, name, age, gender) values(?, ?, ?, ?)")
    }

    override def invoke(value: Student, context: SinkFunction.Context[_]): Unit = {
        println("updating...")
        update.setInt(1, value.age)
        update.setString(2, value.id)
        update.execute()

        if (update.getUpdateCount == 0) {
            println("insert...")
            insert.setString(1, value.id)
            insert.setString(2, value.name)
            insert.setInt(3, value.age)
            insert.setString(4, value.gender.toString)
            insert.execute()
        }
    }

    override def close(): Unit = {
        println("closing...")
        update.close()
        insert.close()
        con.close()
    }
}