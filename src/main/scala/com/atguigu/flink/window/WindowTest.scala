package com.atguigu.flink.window

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark._
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}

object WindowTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //使用事件时间
    val inputDataStream: DataStream[String] = env.readTextFile("input/wc.log")

    val winStream = inputDataStream.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .countWindow(2)
    winStream.sum(1).print()
    env.execute()
  }
}

case class Visit(id: String, count: Int, timestamp: Long)

object WindowTest1 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val inputDataStream: DataStream[String] = env.socketTextStream("hadoop102", 44444)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //使用eventTime
    //env.getConfig.setAutoWatermarkInterval(1000) //设置自动生成watermark的时间
    env.setParallelism(2)

    val visitDataStream: DataStream[Visit] = inputDataStream.map(line => {
      val data: Array[String] = line.split(" ")
      Visit(data(0), data(1).trim.toInt, data(2).trim.toLong * 1000)
    })

    // 设置时间戳和水位线, 延时1s  BoundedOutOfOrdernessTimestampExtractor类是一个提供好的Watermark的引入, 自定义的再下面
    val watermarkStream = visitDataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Visit](Time.seconds(1)) {
      override def extractTimestamp(element: Visit): Long = element.timestamp
    })

    watermarkStream.print("input")

    watermarkStream.map(v => (v.id, v.count)).keyBy(0)
      .timeWindow(Time.seconds(15), Time.seconds(5))
      .reduce((d1, d2) => (d1._1, d1._2.max(d2._2)))
      .print("max")

    env.execute()
  }
}

// 周期性生成watermark
class MyAssign extends AssignerWithPeriodicWatermarks[Visit] {
  val bound: Long = 60 * 1000 //设置延时时间为1分钟

  var maxTs: Long = 0 //观察到的最大的时间戳

  // 当前的水位线
  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTs - bound) //水位线为当前最大的时间减一分钟
  }

  // 当前事件的时间
  override def extractTimestamp(element: Visit, previousElementTimestamp: Long): Long = {
    maxTs = maxTs.max(element.timestamp)
    element.timestamp
  }
}

// 间断式的生成watermark
class MyAssign1 extends AssignerWithPunctuatedWatermarks[Visit] {
  override def checkAndGetNextWatermark(lastElement: Visit, extractedTimestamp: Long): Watermark = {
    //当id为1是生成watermark, 否则不生成
    if (lastElement.id == "1") new Watermark(extractedTimestamp - 60 * 1000) //延时1分钟
    else null
  }

  override def extractTimestamp(element: Visit, previousElementTimestamp: Long): Long = element.timestamp
}