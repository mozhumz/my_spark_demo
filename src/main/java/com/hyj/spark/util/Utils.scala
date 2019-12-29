package com.hyj.spark.util

import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import com.huaban.analysis.jieba.{JiebaSegmenter, SegToken}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}

class Utils {

  def jieba_seg(df:DataFrame,colName:String):DataFrame={
    val spark = df.sparkSession
    //    结巴对sentence进行分词
    val segmenter = new JiebaSegmenter()
    //    将对应结巴类创建broadcast
    val broadcastSeg = spark.sparkContext.broadcast(segmenter)

    val jiebaUdf = udf{(sentence:String)=>
      val exeSegmenter = broadcastSeg.value
      exeSegmenter.process(sentence.toString,SegMode.INDEX)
        .toArray().map(_.asInstanceOf[SegToken].word)
        .filter(_.length>1).mkString("/")
    }
    df.withColumn("seg",jiebaUdf(col(colName)))
  }

}
