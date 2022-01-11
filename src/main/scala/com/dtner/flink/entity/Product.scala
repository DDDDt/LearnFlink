package com.dtner.flink.entity

/**
 * 产品
 * @param name 产品名
 * @param shelfLife 保质期
 * @param productionDate 生产日期
 */
case class Product(name: String, shelfLife: Int, productionDate: Long)
