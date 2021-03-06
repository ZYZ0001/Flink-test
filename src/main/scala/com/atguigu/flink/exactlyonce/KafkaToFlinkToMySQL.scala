package com.atguigu.flink.exactlyonce

import java.util.Properties

import com.esotericsoftware.kryo.serializers.DefaultSerializers.StringSerializer
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.shaded.zookeeper.org.apache.zookeeper.server.quorum.QuorumCnxManager.Message
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerConfig

//Kafka->Flink->MySQL实现exactly-once语义
object KafkaToFlinkToMySQL {
  def main(args: Array[String]): Unit = {
    // 获取执行环境并设置
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置并行的为1, 方便查看消息顺序, 可以改为多并行度
    env.setParallelism(1)
    // 启动检查点, 并设置5s启动一个
    env.enableCheckpointing(5000)
    // 设置检查点的exactly_once语义, 上面在启动检查点时会自动设置
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 设置检查点间最小的时间间隔
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(100)
    // 设置最大的时间间隔, 超时5s将丢弃检查点
    env.getCheckpointConfig.setCheckpointTimeout(5000)
    // 同一时间只允许进行一次检查点, 设置最小的时间后, 这个配置也就无效了
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 表示一旦Flink程序被cancel后，会保留checkpoint数据，以便根据实际需要恢复到指定的checkpoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    // 设置检查点出错是否停止应用, 默认true
        env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    // 设置状态后端存储位置
    env.setStateBackend(new FsStateBackend("file:///D:\\\\IDEA\\\\ideaU\\\\IdeaProjects\\\\flink-tutorial\\\\checkpoint"))
    // 设置job出错重启次数 1分钟重启3次
    //    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 60000))

//    env.getConfig.addDefaultKryoSerializer(classOf[String], classOf[StringSerializer])
    // 获取Kafka输入流
    val properties = new Properties()
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092")
    // earliest:从最早的offset开始拉取，latest:从最近的offset开始消费
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false") //关闭自动提交offset
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "flink_consumer2")
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    // 设置隔离级别为读取已提交的, read_uncommitted可以读取未提交的(脏读)
    properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
//        val kafkaInputDataStream = env.addSource(new FlinkKafkaConsumer011("kafkaExactlyOnce", new JSONKeyValueDeserializationSchema(true), properties))
    val kafkaInputDataStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("kafkaExactlyOnce", new SimpleStringSchema(), properties))
    // 测试输出
    kafkaInputDataStream.print()
    // 添加Sink, 输出到MySQL
    kafkaInputDataStream.addSink(new MySQLTwoPhaseCommitSink()).name("MySQLTwoPhaseCommitSink")

    env.execute("test exactly-once")
  }
}