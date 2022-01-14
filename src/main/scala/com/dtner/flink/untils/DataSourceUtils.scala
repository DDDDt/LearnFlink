package com.dtner.flink.untils

import com.dtner.flink.entity.ProductInfo


/**
 * @program: com.learn.flink
 * @description: 获取数据
 * @author: dt
 * @create: 2022-01-10
 * */
object DataSourceUtils {

  /**
   * 获取数据
   * @return
   */
  def getArrayDatas(): Array[ProductInfo] = {

    Array.apply(ProductInfo("面包", 60, 1641865883869L), ProductInfo("酸奶", 30, 1641865884869L), ProductInfo("蛋挞", 40, 1641865893869L),
      ProductInfo("牛奶", 90,1641865885869L), ProductInfo("可乐", 20, 1641865887869L), ProductInfo("千层", 4, 1641865889869L),
      ProductInfo("香蕉", 8, 1641865881869L), ProductInfo("苹果", 18, 1641865882869L), ProductInfo("蛋糕", 9, 1641865888869L))

  }


}
