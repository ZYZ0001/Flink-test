package com.atguigu.flink.api

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests


object SilkToES {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        val inputDataStream: DataStream[Student] = env.fromCollection(List(
            Student("1001", "xiaoming", 13, 'F'),
            Student("1002", "xiaochen", 16, 'F'),
            Student("1003", "xiaohuang", 13, 'M'),
            Student("1004", "xiaohong", 15, 'M'),
            Student("1001", "xiaoming", 14, 'M'),
            Student("1002", "xiaochen", 17, 'F')
        ))

        val hosts = new java.util.ArrayList[HttpHost]()
        hosts.add(new HttpHost("hadoop102", 9200))

        val esBuilder = new ElasticsearchSink.Builder[Student](hosts, new ElasticsearchSinkFunction[Student] {
                override def process(t: Student, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
                val map = new java.util.HashMap[String, Any]()
                map.put("id", t.id)
                map.put("name", t.name)
                map.put("age", t.age)
                map.put("gender", t.gender.toString)
                println(map)
                val request: IndexRequest = Requests.indexRequest().index("student_test").`type`("_doc").id(t.id).source(map)
                requestIndexer.add(request)
                println("保存成功")
            }
        })

        inputDataStream.addSink(esBuilder.build())




        env.execute()
    }
}
