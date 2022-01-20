package com.dtner.flink

import com.dtner.flink.sink.KafkaSinkObj
import com.dtner.flink.source.KafkaSourceObj
import com.dtner.flink.untils.ParameterToolUtils
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
/**
 * @program: com.learn.flink
 * @description: Flink 流管理
 * @author: dt
 * @create: 2022-01-19
 * */
object FlinkStreamStore {

  /**
   * Flink 流管理
   * @param args
   */
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 获取外置配置文件
    val parameterTool = ParameterToolUtils.getParameterTool(args)

    // 从 kafka 获取数据来源
    val flinkKafkaSource = KafkaSourceObj.getKafkaConsumer(parameterTool)
    val stream = env.fromSource(flinkKafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source")
      .uid("flink-kafka-source")
      .name("flink-kafka-source")

    val mapStream = stream.map(_.toUpperCase)
      .uid("flink-operator").name("flink-operator")

    mapStream.print()


    mapStream.sinkTo(KafkaSinkObj.getKafkaProducer(parameterTool))

    env.execute("flink-stream-job")


  }

}
