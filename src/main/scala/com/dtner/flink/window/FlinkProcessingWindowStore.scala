package com.dtner.flink.window

import com.dtner.flink.process.WindowMaxProcess
import com.dtner.flink.untils.DataSourceUtils
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time


/**
 * @program: com.learn.flink
 * @description: Flink 窗口处理时间
 * @author: dt
 * @create: 2022-01-10
 * */
object FlinkProcessingWindowStore {

  /**
   * 具体的处理方法
   * @param args
   */
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val datas = DataSourceUtils.getArrayDatas()

    // flink source
    val stream = env.fromElements(datas: _*).uid("source-array").name("source-array")

    // 滚动处理时间窗口
    val windowStream = stream.keyBy(k => k.name)
      .window(TumblingProcessingTimeWindows.of(Time.milliseconds(1)))
      .process(new WindowMaxProcess)
      .uid("operator-window")
      .name("operator-window")


    windowStream.print()

    env.execute("flink-window-store")

  }


}
