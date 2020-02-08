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
    val df = spark.sql("select * from badou.news_seg")


//    结巴对sentence进行分词
    val segmenter = new JiebaSegmenter()
//    将对应结巴类创建broadcast 分发变量segmenter到各节点
    val broadcastSeg = spark.sparkContext.broadcast(segmenter)

    val jiebaUdf = udf{(sentence:String)=>
      val exeSegmenter = broadcastSeg.value
      //切分 得到每个单词的索引
      exeSegmenter.process(sentence.toString,SegMode.INDEX)
        .toArray()
        //根据索引得到单词
        .map(_.asInstanceOf[SegToken].word)
        //获取单词长度大于1的
        .filter(_.length>1)
        //返回值为String
        .mkString("/")
    }


    val df_seg = df.withColumn("seg",jiebaUdf(col("sentence")))
    df_seg.show(50)

//    val JiebaUtils = new Utils()
//    val df_seg_utils = JiebaUtils.jieba_seg(df,"sentence")
//    df_seg_utils.show()

  }

}
