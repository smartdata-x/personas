package com.kunyan.userportrait

import com.kunyan.userportrait.config.{PlatformConfig, FileFormatConfig, SparkConfig}
import com.kunyan.userportrait.data.Analysis
import com.kunyan.userportrait.rule.url._
import com.kunyan.userportrait.util.FileUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by yangshuai on 2016/2/24.
  */
object Extractor {

  val sparkConf = new SparkConf().setMaster("local")
    .setAppName("USER PORTRAIT").set("spark.serializer",SparkConfig.SPARK_SERIALIZER).set("spark.kryoserializer.buffer.max",SparkConfig.SPARK_KRYOSERIALIZER_BUFFER_MAX)
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)

  def main(args: Array[String]) {

    if(args.length < 1){
      System.err.println("Extractor data_files")
      System.exit(-1)
    }
    val dataRdd = sc.textFile(args(0))
    Analysis.loadData(dataRdd,FileFormatConfig.tableName)

    // ele.me
    val eleme  = Analysis.getAdAndUaAndUrl(PlatformConfig.PLATFORM_ELEME)
    eleme.foreach(EleMe.extract)
    EleMe.urlListBuffer.distinct.toArray.foreach(x => println("eleme:"+x))
    FileUtil.saveAdAndUaAndUrl(EleMe.urlListBuffer.distinct.toArray,PlatformConfig.PLATFORM_ELEME,2)
    // zhihu.com
    val zhihu  = Analysis.getAdAndUaAndUrl(PlatformConfig.PLATFORM_ZHIHU)
    zhihu.foreach(ZhiHu.extract)
    FileUtil.saveAdAndUaAndUrl(ZhiHu.urlListBuffer.distinct.toArray,PlatformConfig.PLATFORM_ZHIHU,2)
    // weibo.com
    val weibo  = Analysis.getAdAndUaAndUrl(PlatformConfig.PLATFORM_WEIBO)
    weibo.foreach(WeiBo.WeiBo)
    FileUtil.saveAdAndUaAndUrl(WeiBo.urlListBuffer.distinct.toArray,PlatformConfig.PLATFORM_WEIBO,2)
    // suning.com
    val suning  = Analysis.getAdAndUaAndUrl(PlatformConfig.PLATFORM_SUNING)
    suning.foreach(SuNing.extract)
    FileUtil.saveAdAndUaAndUrl(SuNing.urlListBuffer.distinct.toArray,PlatformConfig.PLATFORM_SUNING,2)
    // qq
    val qq  = Analysis.getAdAndUaAndUrl(PlatformConfig.PLATFORM_QZONE)
    qq.foreach(Qzone.QQzone)
    FileUtil.saveAdAndUaAndUrl(Qzone.urlListBuffer.distinct.toArray ,PlatformConfig.PLATFORM_QZONE,2)
    FileUtil.saveAdAndUaAndUrl(Qzone.QQListBuffer.distinct.toArray ,PlatformConfig.PLATFORM_QZONE,1)
    sc.stop()

  }

}
