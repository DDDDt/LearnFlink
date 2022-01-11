package com.dtner.flink.untils

import com.dtner.flink.entity.Product


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
  def getArrayDatas(): Array[Product] = {

    Array.apply(Product("面包", 60, 1641865883869L), Product("酸奶", 30, 1641865884869L), Product("蛋挞", 40, 1641865893869L),
      Product("牛奶", 90,1641865885869L), Product("可乐", 20, 1641865887869L), Product("千层", 4, 1641865889869L),
      Product("香蕉", 8, 1641865881869L), Product("苹果", 18, 1641865882869L), Product("蛋糕", 9, 1641865888869L))

  }


}
