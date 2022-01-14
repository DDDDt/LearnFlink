package com.dtner.flink.process

import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import com.dtner.flink.entity.ProductInfo

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

/**
 * @program: com.learn.flink
 * @description: ${description}
 * @author: dt
 * @create: 2022-01-13
 * */
class PseudoWindow extends ProcessFunction[ProductInfo, Int]{

  /**
   * 保存状态
   */
  @transient private var mapState: MapState[String, Int] = _
  /**
   * 保存窗口时间戳状态
   */
  @transient private var timeMapState: MapState[Long, Int] = _

  /**
   * 处理逻辑
   * @param value
   * @param ctx
   * @param out
   */
  override def processElement(value: ProductInfo, ctx: ProcessFunction[ProductInfo, Int]#Context, out: Collector[Int]): Unit = {

    if (!mapState.contains(value.name)) {
      mapState.put(value.name, value.shelfLife)
      out.collect(value.shelfLife)
    } else {
      val shelfLife = mapState.get(value.name)
      mapState.put(value.name, value.shelfLife + shelfLife)
      out.collect(value.shelfLife + shelfLife)
    }

  }

  /**
   * 窗口需要完成的时候调用
   * @param timestamp
   * @param ctx
   * @param out
   */
  override def onTimer(timestamp: Long, ctx: ProcessFunction[ProductInfo, Int]#OnTimerContext, out: Collector[Int]): Unit = {

    val sumValue = timeMapState.values().asScala.sum
    out.collect(sumValue)

  }

  /**
   * 初始化调用一次
   * @param parameters
   */
  override def open(parameters: Configuration): Unit = {

    val mapStateDes = new MapStateDescriptor[String, Int]("process-window-state-map", classOf[String], classOf[Int])
    val timeMapStateDes = new MapStateDescriptor[Long, Int]("process-window-time-state-map", classOf[Long], classOf[Int])
    mapState = getRuntimeContext.getMapState(mapStateDes)
    timeMapState = getRuntimeContext.getMapState(timeMapStateDes)

  }
}
