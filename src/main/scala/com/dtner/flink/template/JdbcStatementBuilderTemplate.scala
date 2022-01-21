package com.dtner.flink.template

import org.apache.flink.connector.jdbc.JdbcStatementBuilder

import java.sql.PreparedStatement

/**
 * @program: com.learn.flink
 * @description: JDBC 模板类
 * @author: dt
 * @create: 2022-01-20
 * */
class JdbcStatementBuilderTemplate[T] extends JdbcStatementBuilder[T]{

  override def accept(t: PreparedStatement, u: T): Unit = {


  }
  
}
