package com.atguigu.flink.processandstate

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

//同一的传感器在最近两秒内温度连续上升报警 -- 使用ProcessFunction 最底层的API, 带有所有state/watermark
object ProcessTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //启动EventTime
    val inputDataStream: DataStream[String] = env.socketTextStream("localhost", 44444)
    val keyedStream = inputDataStream.map(line => {
      val data = line.split(" ");
      Sensor(data(0), data(1).toDouble, data(2).toLong)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Sensor](Time.seconds(1)) {
        // 设置EventTime在事件中的位置， 并启动watermark, 设置1s的延时用于处理乱序数据
        override def extractTimestamp(element: Sensor): Long = element.timestamp * 1000
      })
      .keyBy(_.id)

    val processDataStream = keyedStream.process(new MyAlertProcessFunction(5))

    keyedStream.print("input")
    processDataStream.print("alter")

    env.execute()
  }
}
class MyAlertProcessFunction(intervalSecondTime: Int) extends KeyedProcessFunction[String, Sensor, String] {
  // 定义一个状态用来保存上条数据的温度
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  // 定义一个状态用来保存现在的时间戳
  lazy val tsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("lastTime", classOf[Long]))
  // 定义一个状态用来判断是否为第一条数据
  lazy val notIsFirstState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isFirst", classOf[Boolean]))

  // 对数据的处理, ctx可以获取state/watermark/timer
  override def processElement(value: Sensor, ctx: KeyedProcessFunction[String, Sensor, String]#Context, out: Collector[String]): Unit = {
    // 获取上次的状态
    val lastTemp: Double = lastTempState.value()
    val ts: Long = tsState.value()
    val notIsFirst: Boolean = notIsFirstState.value()
    // 更新温度状态
    lastTempState.update(value.temperature)

    if (!notIsFirst || value.temperature < lastTemp) { //如果是第一条数据或者温度下降
      // 清空之前的状态
      ctx.timerService().deleteProcessingTimeTimer(ts)
      tsState.clear()
      // 保存当前状态
      notIsFirstState.update(true)
      lastTempState.update(value.temperature)
    } else if (lastTemp < value.temperature && ts == 0) { //如果温度上升且没有设置定时器
      // 开启定时器
      val time = ctx.timerService().currentProcessingTime() + intervalSecondTime * 1000 //最近n秒
      tsState.update(time)
      ctx.timerService().registerProcessingTimeTimer(time)
    }
  }

  // 定时器触发操作
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Sensor, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 输出报警
    out.collect(ctx.getCurrentKey + "在" + timestamp + "温度连续上升")
    // 清空状态
    tsState.clear()
  }
}