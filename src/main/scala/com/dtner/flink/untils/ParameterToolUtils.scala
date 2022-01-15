package com.dtner.flink.untils

import org.apache.flink.api.java.utils.ParameterTool
import org.slf4j.{Logger, LoggerFactory}

/**
 * @program: com.learn.flink
 * @description: 获取外部配置文件信息
 * @author: dt
 * @create: 2022-01-15
 * */
object ParameterToolUtils {

  private val log: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * 获取配置参数 parameterTool
   * @param arg
   * @return
   */
  @throws[Exception]
  def getParameterTool(arg: Array[String]): ParameterTool = {

    val argParameter = ParameterTool.fromArgs(arg)
    if (!argParameter.has("configFile")) {
      log.error(s"没有配置配置文件地址")
      throw new Exception(s"未获取到 configFile 参数配置的配置文件地址")
    }
    val configFile = argParameter.get("configFile")
    ParameterTool.fromPropertiesFile(configFile)

  }


}
