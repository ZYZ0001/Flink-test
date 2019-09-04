package com.atguigu.flink.processandstate

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

case class Sensor(id: String, temperature: Double, timestamp: Long)

//同一的传感器在最近两次数据差值在10度以上的报警 -- 使用状态编程 和 富函数
object StateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputDataStream: DataStream[String] = env.socketTextStream("localhost", 44444)
    val keyedStream: KeyedStream[Sensor, String] = inputDataStream.map(line => {
      val data = line.split(" ");
      Sensor(data(0), data(1).toDouble, data(2).toLong)
    }).keyBy(_.id)

    val alertDataStream = keyedStream.flatMap(new MyAlertFlatMapFunction)

    keyedStream.print("input")
    alertDataStream.print("alert")
    env.execute("StateTest")
  }
}

class MyAlertFlatMapFunction extends RichFlatMapFunction[Sensor, (String, Double, Double)] {

  private var lastState: ValueState[Double] = _ //定义上次数据的状态

  // open方法只在第一条数据进入时调用一次
  override def open(parameters: Configuration): Unit = {
    lastState = getRuntimeContext.getState[Double](new ValueStateDescriptor[Double]("lastState", classOf[Double]))
  }

  override def flatMap(value: Sensor, out: Collector[(String, Double, Double)]): Unit = {
    // 获取上次温度数据
    val lastTemp: Double = lastState.value()
    // 计算差值
    val diff = (value.temperature - lastTemp).abs
    // 判断
    if (diff > 10) {
      // 要输出的数据
      out.collect(("最近两次温度差值过大", value.temperature, lastTemp))
    }
    // 更新状态
    lastState.update(value.temperature)
  }
}

//同一的传感器在最近两次数据差值在10度以上的报警 -- 便捷方式
object StateTest1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputDataStream: DataStream[String] = env.socketTextStream("localhost", 44444)
    val keyedStream: KeyedStream[Sensor, String] = inputDataStream.map(line => {
      val data = line.split(" ");
      Sensor(data(0), data(1).toDouble, data(2).toLong)
    }).keyBy(_.id)

    val alertDataStream: DataStream[(String, Double, Double)] = keyedStream.flatMapWithState[(String, Double, Double), Double] {
      case (in, None) => (List.empty, Some(in.temperature))
      case (in, state: Some[Double]) => {
        val lastTemp = state.get
        val diff = (in.temperature - lastTemp).abs
        if (diff > 10) (List(("最近两次温差过大", in.temperature, lastTemp)), Some(in.temperature))
        else (List.empty, Some(in.temperature))
      }
    }
    keyedStream.print("input")
    alertDataStream.print("alert")

    env.execute("StateTest1")
  }
}
