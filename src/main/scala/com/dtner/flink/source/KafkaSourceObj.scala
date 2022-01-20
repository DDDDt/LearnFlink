package com.dtner.flink.source

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.connector.kafka.source.KafkaSource
import java.util.Properties
import scala.collection.JavaConverters._

/**
 * @program: com.learn.flink
 * @description: ${description}
 * @author: dt
 * @create: 2022-01-19
 * */
object KafkaSourceObj {

  /**
   * 获取 kafka consumer
   * @param parameterTool
   * @tparam T
   * @return
   */
  def getKafkaConsumer(parameterTool: ParameterTool): KafkaSource[String] = {

    val properties = new Properties()
    parameterTool.toMap.asScala.foreach(kv => {
      if (kv._1.startsWith("kafka.consumer.")) {
        properties.put(kv._1.replace("kafka.consumer.",""), kv._2)
      }
    })

    KafkaSource.builder[String]()
      .setTopics(parameterTool.get("kafka.consumer.topics").split(","): _*)
      .setProperties(properties)
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

  }


}
