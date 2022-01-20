package com.dtner.flink.sink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}

/**
 * @program: com.learn.flink
 * @description: kafka sink
 * @author: dt
 * @create: 2022-01-19
 * */
object KafkaSinkObj {

  def getKafkaProducer(parameterTool: ParameterTool): KafkaSink[String] = {

    KafkaSink.builder[String]()
      .setBootstrapServers(parameterTool.get("kafka.producer.bootstrap.servers"))
      .setRecordSerializer(KafkaRecordSerializationSchema.builder[String]()
      .setTopic(parameterTool.get("kafka.producer.topics"))
      .setValueSerializationSchema(new SimpleStringSchema()).build())
      .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .build()


  }

}
