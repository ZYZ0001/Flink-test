package com.atguigu.flink.exactlyonce

import java.sql.{Connection, PreparedStatement, Timestamp}

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeutils.base.VoidSerializer
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.functions.sink.{SinkFunction, TwoPhaseCommitSinkFunction}

/**
  * 自定义Flink -> MySQL, 继承TwoPhaseCommitSinkFunction实现两阶段提交
  */
class MySQLTwoPhaseCommitSink extends TwoPhaseCommitSinkFunction[String, Connection, Void](new KryoSerializer(classOf[Connection], new ExecutionConfig()), VoidSerializer.INSTANCE) {
  /**
    * 执行数据库入库操作, task初始化的时候调用
    *
    * @param transaction
    * @param value
    * @param context
    */
  override def invoke(transaction: Connection, value: String, context: SinkFunction.Context[_]): Unit = {
    // 获取数据
//    val dataValue: String = value.toString
//    val valueJson: JSONObject = JSON.parseObject(dataValue, classOf[JSONObject])
//    val value_String: String = valueJson.get("value").toString
    val value_String = value
    // 编写sql语句, 并发送数据
    val sql = "insert into exactly_once_test(value, insert_time) values(?, ?)"
    val ps: PreparedStatement = transaction.prepareStatement(sql)
    ps.setString(1, value_String)
    val value_time = new Timestamp(System.currentTimeMillis())
    ps.setTimestamp(2, value_time)
    println(s"要插入的数据:{$value_String}--{$value_time}")
    ps.execute()

    // 手动制造异常用于测试
    //    if (value_String.toInt == 15) println(1 / 0)
  }

  /**
    * 获取连接, 开启手动提交事务
    *
    * @return
    */
  override def beginTransaction(): Connection = {
    println("start beginTransaction...")
    val url = "jdbc:mysql://hadoop102:3306/test?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false&autoReconnect=true&serverTimezone=UTC"
    DBConnectUtil.getConnection(url, "root", "password")
  }

  /**
    * 预提交, 这里预提交的方法在invoke中
    *
    * @param transaction
    */
  override def preCommit(transaction: Connection): Unit = {
    println("start preCommit...")
  }

  /**
    * 正常提交
    * 如果invoke方法正常执行, 则提交事务
    *
    * @param transaction
    */
  override def commit(transaction: Connection): Unit = {
    println("start commit...")
    DBConnectUtil.commit(transaction)
  }

  /**
    * 异常中断
    * 如果invoke方法执行异常, 则回滚事务, 下次的checkpoint操作也不会执行
    *
    * @param transaction
    */
  override def abort(transaction: Connection): Unit = {
    println("start abort rollback...")
    DBConnectUtil.rollback(transaction)
  }
}
