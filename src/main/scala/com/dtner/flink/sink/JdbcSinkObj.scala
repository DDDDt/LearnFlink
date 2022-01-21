package com.dtner.flink.sink

import com.dtner.flink.entity.ProductInfo
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.functions.sink.SinkFunction

import java.sql.PreparedStatement

/**
 * @program: com.learn.flink
 * @description: Jdbc sink
 * @author: dt
 * @create: 2022-01-20
 * */
object JdbcSinkObj {

  /**
   * 获取 jdbc 连接
   * @param parameterTool
   * @return
   */
  def getJdbcSink(parameterTool: ParameterTool): SinkFunction[ProductInfo] = {

    JdbcSink.sink(parameterTool.getRequired("flink.sql"), new JdbcStatementBuilder[ProductInfo] {
      override def accept(t: PreparedStatement, u: ProductInfo): Unit = {
        t.setString(1, u.name)
        t.setInt(2, u.productArity)
        t.setLong(3, u.productionDate)
      }
    }, new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
      .withUrl(parameterTool.getRequired("flink.database.url"))
      .withDriverName(parameterTool.getRequired("flink.database.driver"))
      .withUsername(parameterTool.getRequired("flink.database.username"))
      .withPassword(parameterTool.getRequired("flink.database.pwd"))
      .build()
    )

  }

}
