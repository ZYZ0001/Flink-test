package com.atguigu.flink.exactlyonce

import java.sql.{Connection, DriverManager}

/**
  * 连接数据库的类
  */
object DBConnectUtil {
  /**
    * 获取于MySQL的连接
    *
    * @param url
    * @param user
    * @param password
    * @return
    */
  def getConnection(url: String, user: String, password: String): Connection = {
    var conn: Connection = null
    try {
      Class.forName("com.mysql.jdbc.Driver")
      conn = DriverManager.getConnection(url, user, password)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    // 设置手动提交
    conn.setAutoCommit(false)
    conn
  }

  /**
    * 提交事务
    *
    * @param conn
    */
  def commit(conn: Connection): Unit = {
    if (conn != null) {
      try {
        conn.commit()
        println("成功提交事务")
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        close(conn)
      }
    }
  }

  /**
    * 事务回滚
    *
    * @param conn
    */
  def rollback(conn: Connection): Unit = {
    if (conn != null) {
      try {
        conn.rollback()
        println("成功回滚事务")
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        close(conn)
      }
    }
  }

  /**
    * 关闭连接
    *
    * @param conn
    */
  def close(conn: Connection): Unit = {
    if (conn != null) {
      try {
        conn.close()
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }
}
