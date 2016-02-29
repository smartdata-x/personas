package com.kunyan.userportrait.Test

import com.kunyan.userportrait.config.{FileFormatConfig, SparkConfig, PlatformConfig}
import com.kunyan.userportrait.data.Analysis
import com.kunyan.userportrait.util.FileUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by C.J.YOU on 2016/2/29.
  */
object Test {
  val sparkConf = new SparkConf().setMaster("local")
    .setAppName("USER PORTRAIT").set("spark.serializer",SparkConfig.SPARK_SERIALIZER).set("spark.kryoserializer.buffer.max",SparkConfig.SPARK_KRYOSERIALIZER_BUFFER_MAX)
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)
  def main(args: Array[String]) {
    val dataRdd = sc.textFile(args(0))
    Analysis.createDataTable(dataRdd,FileFormatConfig.tableName)
    val result = Analysis.getAdAndUaAndUrl(PlatformConfig.PLATFORM_ELEME).collect()
    FileUtil.saveAdAndUaAndUrl(result,PlatformConfig.PLATFORM_SUNING,1)
  }
}
