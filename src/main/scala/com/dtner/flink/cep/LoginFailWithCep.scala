package com.dtner.flink.cep

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

/**
 * @program: com.learn.flink
 * @description: 用户连续登录判断
 * @author: dt
 * @create: 2022-04-13
 *
 * */

case class LoginInfo(userId: Int, userName: String, loginState: String, loginTime: Long)

object LoginFailWithCep {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val resource = getClass.getResource("/data/UserLogin.txt")
    env.setParallelism(1)

    // 定义读取文件
    val loginEventSource = env.readTextFile(resource.getPath)
      .uid("login-event-source")
      .name("login-event-source")

    // 转换为 case class
    val loginEventStream = loginEventSource.map(record => {
      implicit val formats = DefaultFormats
      JsonMethods.parse(record).extract[LoginInfo]
    }).keyBy(_.userId)

    // 个体模式
    val perPattern = Pattern.begin[LoginInfo]("start").where(_.loginState.equals("fail"))
      .times(5).within(Time.seconds(5))

    // 定义匹配模式
    val loginFailPattern = Pattern.begin[LoginInfo]("start").where(_.loginState.equals("success"))
      .followedByAny("next").where(_.loginState.equals("fail"))

    /**
     * 在事件流上应用模式，得到一个 pattern stream
     * Flink在1.12版本之后，PatternStream默认使用Event Time。如果业务使用的事Processing Time，必须要明确配置。
     */
    val patternStream = CEP.pattern(loginEventStream, perPattern).inProcessingTime()
//    val patternStream = CEP.pattern(loginEventStream, loginFailPattern).inProcessingTime()


    patternStream.select{
      map =>
        val firstSuccess= map("start").iterator.next()
        /*
        val lastFail= map("next").iterator.next()
        (firstSuccess,lastFail)
        */
        s"userId: ${firstSuccess.userId}, name: ${firstSuccess.userName},mapSize: ${map.size}"
    }.print("结果：")

    env.execute("flink-login-cep")

  }

}
