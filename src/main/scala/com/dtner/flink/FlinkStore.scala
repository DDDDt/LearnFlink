package com.dtner.flink

import com.dtner.flink.untils.DataSourceUtils
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

/**
 * @program: com.learn.flink
 * @description: flink 启动类
 * @author: dt
 * @create: 2022-01-08
 * */
object FlinkStore {

  /**
   * 启动类
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // flink 启动环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val datas = DataSourceUtils.getArrayDatas()

    // source
    val source = env.fromElements(datas:_*).name("source-1").uid("source-1")

    // keyby
    source.keyBy(k => k.name).print()

    // flink 执行
    env.execute("flink-store")

  }

}
