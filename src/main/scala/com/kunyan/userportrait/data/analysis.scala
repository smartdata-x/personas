package com.kunyan.userportrait.data

import com.kunyan.userportrait.Extractor
import com.kunyan.userportrait.config.{PlatformConfig, FileFormatConfig}
import com.kunyan.userportrait.platform.{PlatformScheduler, Eleme}
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

/**
  * Created by C.J.YOU on 2016/2/24.
  */
object Analysis {

  val sqlContext = Extractor.sqlContext
  def loadData(sc:SparkContext,path:String,tableName:String):Unit ={
    val dataFile = sc.textFile(path)
    val schemaFormat = StructType(FileFormatConfig.schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val rowRDD = dataFile.map(_.split("\t")).map(p =>Row(p(0),p(1),p(2),p(3),p(4)))
    val dataFrame = sqlContext.createDataFrame(rowRDD,schemaFormat)
    dataFrame.registerTempTable(tableName)
  }

  def getAdAndUa: Unit ={
    val result = sqlContext.sql("select distinct ad,ua from "+FileFormatConfig.tableName +" where url like '"  + PlatformScheduler.apply(PlatformConfig.PLATFORM_ELEME).TOP_LEVEL_DOMAIN +"' and cookies != 'NoDef'" ).map(x =>x(0)+"\t"+x(1)).collect()
      .foreach(println)
  }

  def getAdAndUaAndUrl(pType:Int): Array[String] ={
    val result = sqlContext.sql("select distinct org.ad,org.ua,org.url from "+FileFormatConfig.tableName + " org join ( select distinct ad,ua from "+FileFormatConfig.tableName +" where url like '"  + PlatformScheduler.apply(pType).TOP_LEVEL_DOMAIN +" and cookies != 'NoDef') tmp on (tmp.ad = org.ad and tmp.ua = org.ua)" ).map(x =>x(0)+"\t"+x(1)+"\t"+x(2)).collect()
    result
  }
}
