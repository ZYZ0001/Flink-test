package com.atguigu.flink.exactlyonce

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.util.serialization.{JSONKeyValueDeserializationSchema, KeyedSerializationSchemaWrapper}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig

object KafkaToFlinkToKafka1 {
  def main(args: Array[String]): Unit = {
    // 获取执行环境并设置
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置并行的为1, 方便查看消息顺序, 可以改为多并行度
    env.setParallelism(1)
    // 启动检查点, 并设置5s启动一个
    env.enableCheckpointing(5000)
    // 设置检查点的exactly_once语义, 上面再启动检查点时会自动设置
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 设置检查点间最小的时间间隔
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L)
    // 设置最大的时间间隔, 超时5s将丢弃检查点
    env.getCheckpointConfig.setCheckpointTimeout(30000)
    // 同一时间只允许进行一次检查点, 设置最大的时间后, 这个配置也就无效了
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 表示一旦Flink程序被cancel后，会保留checkpoint数据，以便根据实际需要恢复到指定的checkpoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    // 设置检查点出错是否停止应用, 默认true
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    // 设置状态后端存储位置
    //    env.setStateBackend(new FsStateBackend("file:///D:\\IDEA\\ideaU\\IdeaProjects\\flink-tutorial\\checkpoint"))
    env.setStateBackend(new FsStateBackend("file:///D:\\\\IDEA\\\\ideaU\\\\IdeaProjects\\\\flink-tutorial\\\\checkpoint"))
    // 设置job出错重启次数 1分钟重启3次
    //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 60000))

    // 设置Kafka消费参数
    val pro_csc = new Properties()
    pro_csc.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092")
    // earliest:从最早的offset开始拉取，latest:从最近的offset开始消费
    pro_csc.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    pro_csc.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false") //关闭自动提交offset
    pro_csc.put(ConsumerConfig.GROUP_ID_CONFIG, "flink_consumer01")
    pro_csc.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    pro_csc.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    // 设置隔离级别为读取已提交的, read_uncommitted可以读取未提交的(脏读)
    pro_csc.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
    // 动态感知partition数量的变化
    pro_csc.setProperty("flink.partition-discovery.interval-millis", "5000")

    // SimpleStringSchema可以获取到kafka消息
    // JSONKeyValueDeserializationSchema可以获取到消息的key,value，metadata:topic,partition，offset等信息
    val kafkaConsumer = new FlinkKafkaConsumer011("register_topic", new SimpleStringSchema(), pro_csc)
    //    val kafkaConsumer = new FlinkKafkaConsumer011("kafka_flink", new JSONKeyValueDeserializationSchema(true), pro_csc)
//    kafkaConsumer.setCommitOffsetsOnCheckpoints(false) //关闭自动提交, 会忽略enable.auto.commit的设置
    // 添加Flink的source
    val kafkaDataStream = env.addSource(kafkaConsumer)

    // 转化数据格式
    val dataStream: DataStream[String] = kafkaDataStream.map(_.toString)

    // 测试输出
    dataStream.print()

    // 设置Kafka生产参数
    val pro_pdc = new Properties()
    pro_pdc.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092")
    // 设置事务ID
    pro_pdc.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "flink_producer_1")
    // 设置事务超时时间  默认是100毫秒
    pro_pdc.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "5000")
    // 开启幂等机制
    pro_pdc.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    // 重试次数
    pro_pdc.put(ProducerConfig.RETRIES_CONFIG, "2")

    // 添加Flink的sink
    //    dataStream.addSink(new FlinkKafkaProducer011[String](
    //      "flink_kafka",
    //      new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()),
    //      pro_pdc,
    //      FlinkKafkaProducer011.Semantic.EXACTLY_ONCE
    //      //      FlinkKafkaProducer011.Semantic.NONE
    //    ))

    env.execute(KafkaToFlinkToKafka1.getClass.getName)
  }
}
