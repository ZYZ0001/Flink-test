package com.atguigu.flink.api

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object SinkToRedis {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        val inputDataStream: DataStream[Student] = env.fromCollection(List(
            Student("1001", "xiaoming", 13, 'F'),
            Student("1002", "xiaochen", 16, 'F'),
            Student("1003", "xiaohuang", 13, 'M'),
            Student("1004", "xiaohong", 15, 'M'),
            Student("1001", "xiaoming", 14, 'M'),
            Student("1002", "xiaochen", 17, 'F')
        ))

        val redisConf: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("hadoop102").setPort(6379).build()

        inputDataStream.addSink(new RedisSink[Student](redisConf, new MyRedisMapper))


        env.execute()
    }
}

class MyRedisMapper extends RedisMapper[Student] {
    override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.HSET, "student")

    override def getKeyFromData(t: Student): String = t.id

    override def getValueFromData(t: Student): String = t.toString
}
