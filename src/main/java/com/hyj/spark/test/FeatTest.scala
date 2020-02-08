package com.hyj.spark.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object FeatTest {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("feat test").enableHiveSupport().getOrCreate()
    val orders=spark.sql("select * from badou.orders")
    val priors=spark.sql("select * from badou.priors")
    orders.show(20)
    priors.show(20)

    val op = orders.join(priors,"order_id")
      .persist(StorageLevel.MEMORY_AND_DISK)

    val userXprod = op.selectExpr("concat_ws('_',user_id,product_id) as user_prod",
        "order_number","order_id","order_hour_of_day",
        "cast(add_to_cart_order as int) as add_to_cart_order")
    import spark.implicits._
    val lastOrder = userXprod.rdd.map(x=>(x(0).toString,
      (x(1).toString.replaceAll("\\s*", ""),x(2).toString,x(3).toString)))
      .groupByKey()
      .mapValues(_.toArray.maxBy(_._1.toInt))
      .toDF("user_prod","order_num_id")
      .selectExpr("user_prod","cast(order_num_id._1 as int) as max_ord_num",
        "order_num_id._2 as last_order_id",
        "order_num_id._3 as last_order_hour")
    lastOrder.show()


  }
}
