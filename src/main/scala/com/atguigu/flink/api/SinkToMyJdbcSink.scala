package com.atguigu.flink.api

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

object SinkToMyJdbcSink {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        val studentDataStream: DataStream[Student] = env.fromCollection(List(
            Student("1001", "xiaoming", 13, 'F'),
            Student("1002", "xiaochen", 16, 'F'),
            Student("1003", "xiaohuang", 13, 'M'),
            Student("1004", "xiaohong", 15, 'M'),
            Student("1001", "xiaoming", 14, 'M'),
            Student("1002", "xiaochen", 17, 'F')
        ))

        studentDataStream.addSink(new MyJdbcSink)


        env.execute()
    }
}

// 自定义sink, 将数据输出到mysql上
class MyJdbcSink extends RichSinkFunction[Student] {

    // 定义连接器和编译器
    var connect: Connection = _
    var updateStat: PreparedStatement = _
    var insertStat: PreparedStatement = _

    // 初始化连接JDBC
    override def open(parameters: Configuration): Unit = {
//        super.open(parameters)
        connect = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "password")
        updateStat = connect.prepareStatement("update student set age = ? where id = ?")
        insertStat = connect.prepareStatement("insert into student (id, name, age, gender) values(?, ?, ?, ?)")
        println("open...")
    }

    // 更新和插入数据
    override def invoke(value: Student, context: SinkFunction.Context[_]): Unit = {
        // 先更新数据
        updateStat.setInt(1, value.age)
        updateStat.setString(2, value.id)
        updateStat.execute()
        println("update...")

        // 没有更新数据, 插入数据
        if (updateStat.getUpdateCount == 0) {
            insertStat.setString(1, value.id)
            insertStat.setString(2, value.name)
            insertStat.setInt(3, value.age)
            insertStat.setString(4, value.gender.toString)
            insertStat.execute()
            println("insert...")
        }
    }

    // 断开JDBC
    override def close(): Unit = {
        connect.close()
        updateStat.close()
        insertStat.close()
        println("close...")
    }
}