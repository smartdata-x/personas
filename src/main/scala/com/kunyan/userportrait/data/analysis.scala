package com.kunyan.userportrait.data

import com.kunyan.userportrait.Extractor
import com.kunyan.userportrait.config.{PlatformConfig, FileFormatConfig}
import com.kunyan.userportrait.platform.{PlatformScheduler, Eleme}
import com.kunyan.userportrait.util.FileUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

/**
  * Created by C.J.YOU on 2016/2/24.
  */
object Analysis {

  val sqlContext = Extractor.sqlContext
  def loadData(dataFile: RDD[String],tableName:String):Unit ={
    val schemaFormat = StructType(FileFormatConfig.schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val rowRDD = dataFile.map(_.split("\t")).map(p =>Row(p(0),p(1),p(2),p(3),p(4)))
    val dataFrame = sqlContext.createDataFrame(rowRDD,schemaFormat)
    dataFrame.registerTempTable(tableName)
  }

  def getAdAndUa(pType:Int): Unit ={
    val result = sqlContext.sql("select distinct ad,ua,url,cookies from "+FileFormatConfig.tableName +" where url like '"  + PlatformScheduler.apply(pType).TOP_LEVEL_DOMAIN +"' and cookies != 'NoDef' and ua != 'NoDef'" ).map(x =>x(0)+"\t"+x(1)+"\t"+x(2)+"\t"+x(3)).collect()
      //.foreach(println)
    FileUtil.saveAdAndUaAndUrl(result,pType, 1)
  }

  def getAdAndUaAndUrl(pType:Int): RDD[String] ={
    val result = sqlContext.sql("select distinct org.ad,org.ua,org.url,org.cookies from "+FileFormatConfig.tableName + " org join ( select distinct ad,ua from "+FileFormatConfig.tableName +" where url like '"  + PlatformScheduler.apply(pType).TOP_LEVEL_DOMAIN +"') tmp on (tmp.ad = org.ad and tmp.ua = org.ua)" ).map(x =>x(0)+"\t"+x(1)+"\t"+x(2)+"\t"+x(3))
    result
  }
}
