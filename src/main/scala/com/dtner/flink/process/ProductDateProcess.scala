package com.dtner.flink.process

import com.dtner.flink.entity.ProductInfo
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector

/**
 * @program: com.learn.flink
 * @description: 处理过程
 * @author: dt
 * @create: 2022-01-11
 * */
class ProductDateProcess extends ProcessFunction[ProductInfo, Long]{

  /**
   * 处理巨日逻辑
   * @param value
   * @param ctx
   * @param out
   */
  override def processElement(value: ProductInfo, ctx: ProcessFunction[ProductInfo, Long]#Context, out: Collector[Long]): Unit = {

    out.collect(value.productionDate)

  }
}
