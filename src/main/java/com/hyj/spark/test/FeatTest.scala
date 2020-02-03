package com.hyj.spark.test

import org.apache.spark.sql.SparkSession

object FeatTest {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("feat test").enableHiveSupport().getOrCreate()
    val orders=spark.sql("select * from badou.orders")
    val priors=spark.sql("select * from badou.priors")
    orders.show(20)
    priors.show(20)
    orders.count()

  }
}
