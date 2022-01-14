package com.dtner.flink.window

import com.dtner.flink.entity.ProductInfo
import com.dtner.flink.process.WindowMaxProcess
import com.dtner.flink.untils.DataSourceUtils
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration

/**
 * @program: com.learn.flink
 * @description: 事件时间窗口
 * @author: dt
 * @create: 2022-01-11
 * */
object FlinkEventTimeWindowStore {

  /**
   * 处理方法
   * @param args
   */
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 设置 watermark 间隔时间
    env.getConfig.setAutoWatermarkInterval(600000)

    val datas = DataSourceUtils.getArrayDatas()

    val stream = env.fromElements(datas: _*).uid("flink-source").name("flink-source")

    val windowStream = stream
      // 设置 eventTime 字段
      .assignAscendingTimestamps(_.productionDate)

      /**
       * 当流中存在时间乱序问题，引入 watermark, 并设置延迟时间
       * 传入的参数为 watermark 的最大延迟时间（即允许数据迟到的时间）
       * 重写的extractTimestamp方法返回的是设置数据中EventTime的字段，单位为毫秒，需要将时间转换成Long（最近时间为13位的长整形）才能返回
       * 当我们能大约估计到流中的最大乱序时，建议使用此中方式，比较方便
       */
/*      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[Product](Duration.ofMinutes(1L))
        .withIdleness(Duration.ofMillis(1L))
        .withTimestampAssigner(new SerializableTimestampAssigner[Product] {
          override def extractTimestamp(element: Product, recordTimestamp: Long): Long = element.productionDate
        }))*/


      .keyBy(_.name)
      .window(TumblingEventTimeWindows.of(Time.minutes(1L)))
      .process(new WindowMaxProcess)
      .uid("flink-operator-window").name("flink-operator-window")


    windowStream.print()

    env.execute("flink-eventTime-store")


  }

}
