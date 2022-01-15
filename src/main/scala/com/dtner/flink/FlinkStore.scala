package com.dtner.flink

import com.dtner.flink.untils.{DataSourceUtils, ParameterToolUtils}
import org.apache.flink.api.common.RuntimeExecutionMode
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
    // 设置并行度
    env.getConfig.setParallelism(1)
    // 设置缓冲区等待时间
    env.setBufferTimeout(10L)
    // 设置执行模式，是批处理还是流处理或者通过配置来实现 bin/flink run -Dexecution.runtime-mode=BATCH examples/streaming/WordCount.jar
    env.setRuntimeMode(RuntimeExecutionMode.BATCH)

    // 获取外置配置文件
    val parameterTool = ParameterToolUtils.getParameterTool(args)
    // 注册全局参数, 可以在任意富函数中访问参数
    env.getConfig.setGlobalJobParameters(parameterTool)



    val datas = DataSourceUtils.getArrayDatas()

    // source
    val stream = env.fromElements(datas:_*).name("source-1").uid("source-1")

/*    val iterStream = stream.iterate(iter => {
      val pStream = iter.map(p => p.productArity-1)
      val vStream= pStream.filter(_>0)
      val kStream = iter.filter(_.productArity<=0)
      (kStream, vStream)
    })

    iterStream.print()*/


    // keyby
    stream.keyBy(k => k.name).print()

    // flink 执行
    env.execute("flink-store")

  }

}
