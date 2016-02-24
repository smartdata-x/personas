package com.kunyan.userportrait

import com.kunyan.userportrait.config.{PlatformConfig, FileFormatConfig, SparkConfig}
import com.kunyan.userportrait.data.Analysis
import com.kunyan.userportrait.platform.ZhiHu
import com.kunyan.userportrait.util.FileUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by yangshuai on 2016/2/24.
  */
object Extractor {
  val sparkConf = new SparkConf().setMaster("local").setAppName("USER PORTRAIT").set("spark.serializer",SparkConfig.SPARK_SERIALIZER).set("spark.kryoserializer.buffer.max",SparkConfig.SPARK_KRYOSERIALIZER_BUFFER_MAX)
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)
  def main(args: Array[String]) {

    Analysis.loadData(sc,args(0),FileFormatConfig.tableName)

    // ele.me
    val eleme  = Analysis.getAdAndUaAndUrl(PlatformConfig.PLATFORM_ELEME)
    FileUtil.saveAdAndUaAndUrl(eleme,PlatformConfig.PLATFORM_ELEME)
    // zhihu.com
    val zhihu  = Analysis.getAdAndUaAndUrl(PlatformConfig.PLATFORM_ZHIHU)
    FileUtil.saveAdAndUaAndUrl(zhihu,PlatformConfig.PLATFORM_ZHIHU)
    // weibo.com
    val weibo  = Analysis.getAdAndUaAndUrl(PlatformConfig.PLATFORM_WEIBO)
    FileUtil.saveAdAndUaAndUrl(weibo,PlatformConfig.PLATFORM_WEIBO)
    // suning.com
    val suning  = Analysis.getAdAndUaAndUrl(PlatformConfig.PLATFORM_SUNING)
    FileUtil.saveAdAndUaAndUrl(suning,PlatformConfig.PLATFORM_SUNING)
    // qq
    val qq  = Analysis.getAdAndUaAndUrl(PlatformConfig.PLATFORM_QZONE)
    FileUtil.saveAdAndUaAndUrl(qq,PlatformConfig.PLATFORM_QZONE)

  }


}
