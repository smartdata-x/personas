package com.kunyan.userportrait

import com.kunyan.userportrait.config.{PlatformConfig, FileFormatConfig, SparkConfig}
import com.kunyan.userportrait.data.Analysis
import com.kunyan.userportrait.util._

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ListBuffer

/**
  * Created by yangshuai on 2016/2/24.
  */
object Extractor {

  val sparkConf = new SparkConf().setMaster("local")
    .setAppName("USER PORTRAIT").set("spark.serializer",SparkConfig.SPARK_SERIALIZER).set("spark.kryoserializer.buffer.max",SparkConfig.SPARK_KRYOSERIALIZER_BUFFER_MAX)
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)
  val userInfoList = new ListBuffer[String]()
  def main(args: Array[String]) {

    if(args.length < 1){
      System.err.println("Extractor data_files")
      System.exit(-1)
    }
    val dataRdd = sc.textFile(args(0))
    Analysis.createDataTable(dataRdd,FileFormatConfig.tableName)
    val data = Analysis.loadData(1)
    val dataArray = data.map(x => {
      val ad = x.split("\t")(0)
      ad +"\t"+ PhoneUtil.getPhone(x)+"\t"+QQNumberUtil.QQNumber(x)+"\t"+WeiBoUtil.getWeiBoNumber(x)+"\t"+ EmailUtil.email(x)
    }).distinct().collect()
    // saveUser_Info
    FileUtil.saveAdAndUaAndUrl(dataArray,PlatformConfig.USER_INFO,2)
    // savePhoneNumber
    FileUtil.saveAdAndUaAndUrl(dataArray.filter(_.split("\t")(1)!="Nothing"),PlatformConfig.PLATFORM_SUNING,2)
    // saveQQNumber
    FileUtil.saveAdAndUaAndUrl(dataArray.filter(_.split("\t")(2)!="Nothing"),PlatformConfig.PLATFORM_QZONE,2)
    // saveWeiBoId
    FileUtil.saveAdAndUaAndUrl(dataArray.filter(_.split("\t")(3)!="Nothing"),PlatformConfig.PLATFORM_WEIBO,2)
    // get useful url for kid
//    val urlArray = data.map(x => {
//      val ad = x.split("\t")(0)
//      ad +"\t"+UrlUtil.getUrl(x)
//    }).distinct().collect()
//    // saveUrl
//    FileUtil.saveAdAndUaAndUrl(urlArray,PlatformConfig.HTTP_INFO,1)
    sc.stop()

  }

}
