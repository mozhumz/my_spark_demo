package com.hyj.sparksql.test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object WordCountHyj {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkWordCountHyj")
    val ssc = new SparkContext(conf)
    //如果直接写input_dir或者hdfs://master:9000开头 则从hdfs查找 hdfs://master:9000/user/root/input_dir
    val lines: RDD[String] = ssc.textFile("input_dir")
    //    val lines: RDD[String] = ssc.textFile("file:///G:\\idea_workspace\\my_scala_demo\\spark_demo\\input_dir")
    val word_count_rdd: RDD[(String, Int)] = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    word_count_rdd.collect().foreach(println)
  }
}
