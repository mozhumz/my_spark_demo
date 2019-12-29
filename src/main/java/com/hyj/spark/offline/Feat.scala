package com.hyj.spark.offline

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object Feat {
  def main(args: Array[String]): Unit = {
    val conf = Set("spark.sql.broadcastTimeout", "3000000ms")

    val spark = SparkSession.builder().appName("Hyj-spark").enableHiveSupport().getOrCreate()
    spark.sqlContext.setConf("spark.sql.broadcastTimeout","3000000ms")

    val orders = spark.sql("select * from badou.orders")

    val priors = spark.sql("select * from badou.priors").selectExpr("*", "recorded as reordered").drop("recorded")


    /**
      * product feature
      * 统计商品销售量
      * 商品再次被购买量reordered
      * 统计reordered比率prod_rod_rate  avg=sum/count [1,0,1,0,1]  3/5
      *
      */

    val prodCnt = priors.groupBy("product_id").count()
      .withColumnRenamed("count", "prod_cnt")

    val prodRe = priors.selectExpr("product_id", "cast(reordered as int )").groupBy("product_id")
      .agg(
        count("product_id").as("prod_cnt"),
        sum("reordered").as("reordered_cnt"),
        avg("reordered").as("prod_re_rate")
      )

    prodRe.show()

    /**
      * user Feature
      * 1. 每个用户购买订单的平均day间隔  days_since_prior_order
      * 2. 每个用户的总订单数 count  max(order_number)
      * 3. 每个用户购买的product商品去重后的集合数据  collect_set
      * 4. 用户总商品数量以及去重后的商品数量
      * 5. 每个用户平均每个订单有多少商品
      **/
    //对days_since_prior_order判空处理
    val ordersNew = orders.selectExpr("*", "cast(if(days_since_prior_order='',0.0,days_since_prior_order) as int) as dspo").drop("days_since_prior_order").persist(StorageLevel.MEMORY_AND_DISK_SER)
    ordersNew.show()

    /**
      * 1. 每个用户购买订单的平均day间隔  days_since_prior_order
      * * 2. 每个用户的总订单数 count  max(order_number)
      */
    val orderCal = ordersNew.selectExpr("user_id", "dspo").groupBy("user_id")
      .agg(
        avg("dspo"),
        count("user_id").as("order_count")
      )

    orderCal.show()

    val op = orders.join(priors, "order_id")
    op.show()

    val up = op.selectExpr("user_id", "product_id")
//    val a=orders.persist(StorageLevel.MEMORY_AND_DISK_SER)

    /**
      * 3. 每个用户购买的product商品去重后的集合数据  collect_set
      * * 4. 用户总商品数量以及去重后的商品数量
      */
    val userProdCnt1 = up.groupBy("user_id").agg(
      concat_ws(",", collect_set("product_id")).as("product_ids"),
      count("product_id").as("prod_count"),
      countDistinct("product_id").as("prod_discount")
    )
    println("userProdCnt1---------------------------")
    userProdCnt1.show()

    import spark.implicits._

    val userProdCnt2_2 = up.rdd.map(x => (x(0).toString, x(1).toString)).groupByKey().mapValues {
      records =>
        val rs = records.toSet
        (rs.size, records.size,rs.mkString(" "))
    }
    //打印该rdd前10条数据  rdd.collect().foreach {println}
    userProdCnt2_2.take(10).foreach(println)


    val userProdCnt2 = up.rdd.map(x => (x(0).toString, x(1).toString)).groupByKey().mapValues {
      records =>
        //遍历每个key下的value
        val rs = records.toSet
        //每个key下value的返回值：数据类型为元组
        (rs.size, rs.mkString(","), records.size)
    }.toDF("user_id", "tuple").selectExpr("user_id", "tuple._1 as prod_discount", "tuple._2 as product_ids", "tuple._3 as prod_count")

    println("userProdCnt2---------------------------")
    userProdCnt2.show()

    /**
      * 5. 每个用户平均每个订单有多少商品
      */
    //先计算每个订单多少商品
    val orderCnt = priors.selectExpr("order_id", "product_id").groupBy("order_id").count()
      .withColumnRenamed("count", "order_count").persist(StorageLevel.MEMORY_AND_DISK_SER)

    orderCnt.show()
    //再计算每个用户每个订单的商品平均数
    val userOrderAvg = ordersNew.join(orderCnt, "order_id").selectExpr("user_id", "order_id", "order_count").groupBy("user_id").agg(
      avg("order_count").as("order_avg")
    )
    userOrderAvg.show()

    /** user and product Feature :cross feature交叉特征
      * 1. 统计user和对应product在多少个订单中出现（distinct order_id）
      * 2. 特定product具体在购物车（cart）中出现位置的平均位置
      * 3. 最后一个订单id
      * 4. 用户对应product在所有这个用户购买产品量中的占比rate
      * */
    val userXprod = op.selectExpr("concat_ws('_',user_id,product_id) as user_prod", "order_number", "order_id",
      "order_hour_of_day", "cast(add_to_cart_order as int ) as add_to_cart_order")
    userXprod.show()


    //1 2
    userXprod.groupBy("user_prod").agg(
      countDistinct("order_id").as("order_count"),
      avg("add_to_cart_order").as("avg_cart"),
      //用户对应product的数量
      count("user_prod").as("prod_count")
    )

  }

}
