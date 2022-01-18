package com.dtner.flink.table

import com.dtner.flink.untils.DataSourceUtils
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, TableEnvironment}

/**
 * @program: com.learn.flink
 * @description: Flink Table
 * @author: dt
 * @create: 2022-01-18
 * */
object FlinkTableStore {

  /**
   * 具体的业务处理逻辑
   * @param args
   */
  def main(args: Array[String]): Unit = {

    /**
     * 获取 flink table api 的环境
     */
    /*
    val envSetting = EnvironmentSettings.newInstance().inBatchMode()
      .withBuiltInCatalogName("flink-table-catalog")
      .withBuiltInDatabaseName("flink-table-database")
      .build()
    val tableEnv = TableEnvironment.create(envSetting)
*/

    /**
     * 通过 StreamExecutionEnvironment 获取 flink table api 环境
     */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.BATCH)
    val tabEnv = StreamTableEnvironment.create(env)

    val arrays = DataSourceUtils.getArrayDatas()

    val inputTable = tabEnv.fromDataStream(env.fromElements(arrays: _*))
    tabEnv.createTemporaryView("InputTable", inputTable)

    val resultTable = tabEnv.executeSql("SELECT SUM(shelfLife) FROM InputTable")

    resultTable.print()

//    env.execute("flink-table")


  }

}
