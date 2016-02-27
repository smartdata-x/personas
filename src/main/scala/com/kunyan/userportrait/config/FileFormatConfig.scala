package com.kunyan.userportrait.config

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by C.J.YOU on 2016/2/24.
  */
object FileFormatConfig {

  val userRankData = FileConfig.USER_DATA
  val tableName = "loadstamp19"
  val schemaString = "ts ad ua url cookies"

  def formatScema(schema:String):StructType ={
    val schemaFormat = StructType(FileFormatConfig.schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    schemaFormat
  }

  def formatRow(row:Array[String]): Row ={
    var rowFormat = Row.empty
    row.foreach(p =>{
      rowFormat = Row(p(0),p(1),p(2),p(3),p(4))
    })
    rowFormat
  }

}
