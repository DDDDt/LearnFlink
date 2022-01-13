package com.dtner.flink.operator

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration

/**
 * @program: com.learn.flink
 * @description: 富函数带状态操作的 map
 * @author: dt
 * @create: 2022-01-12
 * */
class MapStateOperator extends RichMapFunction[Product, Int]{

  @transient private var valueState: ValueState[Int] = _

  /**
   * 具体的业务逻辑
   * @param value
   * @return
   */
  override def map(value: Product): Int = {
    if (valueState.value() == null) {
      valueState.update(value.productArity)
      value.productArity
    } else {
      valueState.value()*value.productArity
    }
  }

  /**
   *
   * @param parameters
   */
  override def open(parameters: Configuration): Unit = {
    val valueStateDesc = new ValueStateDescriptor("mapvalue-state", Types.INT)
    valueState = getRuntimeContext.getState(valueStateDesc)
  }

  override def close(): Unit = {
    super.close()
    valueState.clear()
  }
}