package com.dtner.flink.process

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import com.dtner.flink.entity.ProductInfo

/**
 * @program: com.learn.flink
 * @description: flink 窗口求最大值
 * @author: dt
 * @create: 2022-01-10
 * */
class WindowMaxProcess extends ProcessWindowFunction[ProductInfo, Int, String, TimeWindow]{

  /**
   * 窗口函数获取最大值
   * @param key
   * @param context
   * @param elements
   * @param out
   */
  override def process(key: String, context: Context, elements: Iterable[ProductInfo], out: Collector[Int]): Unit = {

    out.collect(elements.maxBy(p => p.shelfLife).shelfLife)

  }

}
