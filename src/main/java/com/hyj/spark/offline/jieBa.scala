package com.hyj.spark.offline

import com.huaban.analysis.jieba.{JiebaSegmenter, SegToken}
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object jieBa {
  def main(args: Array[String]): Unit = {
//    定义结巴分词类的序列化
    val conf = new SparkConf()
      .registerKryoClasses(Array(classOf[JiebaSegmenter]))

    val spark = SparkSession
      .builder()
      .appName("Jieba")
      .config(conf)
//      .config("spark.storage.memoryFraction",0.6)
      .enableHiveSupport()
      .getOrCreate()
//    spark shell [client] 实例化sc,spark
    val df = spark.sql("select * from badou.news_noseg")


//    结巴对sentence进行分词
    val segmenter = new JiebaSegmenter()
//    将对应结巴类创建broadcast
    val broadcastSeg = spark.sparkContext.broadcast(segmenter)

    val jiebaUdf = udf{(sentence:String)=>
      val exeSegmenter = broadcastSeg.value
      exeSegmenter.process(sentence.toString,SegMode.INDEX)
        .toArray().map(_.asInstanceOf[SegToken].word)
        //返回值为String
        .filter(_.length>1).mkString("/")
    }


    val df_seg = df.withColumn("seg",jiebaUdf(col("sentence")))
    df_seg.show(50)

//    val JiebaUtils = new Utils()
//    val df_seg_utils = JiebaUtils.jieba_seg(df,"sentence")
//    df_seg_utils.show()

  }

}
