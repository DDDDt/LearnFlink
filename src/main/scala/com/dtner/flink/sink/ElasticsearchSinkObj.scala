package com.dtner.flink.sink

import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.connectors.elasticsearch.{ActionRequestFailureHandler, ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.{ElasticsearchSink, RestClientFactory}
import org.apache.flink.util.ExceptionUtils
import org.apache.http.HttpHost
import org.elasticsearch.ElasticsearchParseException
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.client.{Requests, RestClientBuilder}
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException

import java.util
import scala.collection.JavaConverters._

/**
 * @program: com.learn.flink
 * @description: es sink
 * @author: dt
 * @create: 2022-01-20
 * */
object ElasticsearchSinkObj {

  /**
   * 获取 es sink
   * @param parameterTool
   * @return
   */
  def getEsSink(parameterTool: ParameterTool): ElasticsearchSink[String] = {


    val esHttpPort = parameterTool.getRequired("es.http.port")
    val httpPorts = esHttpPort.split(",").map(httpPort => new HttpHost(httpPort)).toList.asJava

    val esSinkBuilder = new ElasticsearchSink.Builder[String](httpPorts,
      new ElasticsearchSinkFunction[String] {
        override def process(t: String, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {

          val json = new util.HashMap[String, String]()
          json.put("data", t)

          val indexRequest = Requests.indexRequest()
            .index(parameterTool.getRequired("es.index.name"))
            .`type`(parameterTool.get("es.index.type","_doc"))
            .source(json)

          requestIndexer.add(indexRequest)

        }
      })

    // 处理失败的 es 请求
    esSinkBuilder.setFailureHandler(new ActionRequestFailureHandler {
      @throws[Throwable]
      override def onFailure(actionRequest: ActionRequest, throwable: Throwable, i: Int, requestIndexer: RequestIndexer): Unit = {

        if (ExceptionUtils.findThrowable(throwable, classOf[EsRejectedExecutionException]).isPresent) {
          // 队列已满；重新添加文档进行索引
          requestIndexer.add(actionRequest)
        } else if (ExceptionUtils.findThrowable(throwable, classOf[ElasticsearchParseException]).isPresent) {
          // 文档格式错误；简单地删除请求避免 sink 失败
        } else {
          // 对于所有其他失败的请求，失败的 sink
          // 这里的失败只是简单的重新抛出，但用户也可以选择抛出自定义异常
          throw throwable
        }
      }
    })

    // 批量请求的配置：下面的设置使 sink 在接收每个元素之后立即提交，否则这些元素将被缓存起来
    esSinkBuilder.setBulkFlushMaxActions(1)

    // 为内部创建的 Rest 客户端提供一个自定义配置信息的 RestClientFactory
    esSinkBuilder.setRestClientFactory(new RestClientFactory {
      override def configureRestClientBuilder(restClientBuilder: RestClientBuilder): Unit = {

      }
    })

    esSinkBuilder.build()

  }

}
