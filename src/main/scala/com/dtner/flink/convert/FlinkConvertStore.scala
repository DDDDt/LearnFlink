package com.dtner.flink.convert

import com.dtner.flink.operator.MapStateOperator
import com.dtner.flink.untils.DataSourceUtils
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

/**
 * @program: com.learn.flink
 * @description: ETL 转换
 * @author: dt
 * @create: 2022-01-11
 * */
object FlinkConvertStore {

  /**
   * 具体业务逻辑
   * @param args
   */
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val arrayDatas = DataSourceUtils.getArrayDatas()
    val stream = env.fromElements(arrayDatas: _*).uid("flink-source").name("flink-source")

    // etl 转换
    val convertStream = stream.keyBy(_.name).map(new MapStateOperator())
      .uid("flink-convert").name("flink-convert")

    convertStream.print()

    env.execute("flink-convert-store")


  }

}
