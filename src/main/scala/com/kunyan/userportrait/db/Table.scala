package com.kunyan.userportrait.db

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
// "jdbc:mysql://222.73.34.91:3306/jindowin?user=personas&password=personas&useUnicode=true&characterEncoding=utf8"
/**
  * Created by C.J.YOU on 2016/4/26.
  */
object Table extends  Serializable{

  // readData
  def getTableData(sqlc:SQLContext,readConnection:String,tableName:String): Unit ={
    val properties = new Properties()
    properties.setProperty("driver","com.mysql.jdbc.Driver")
    sqlc.read.jdbc(readConnection,tableName,properties).registerTempTable(tableName)
  }

  // writeData
  def writeTableData(dataFrame: DataFrame,writeConnection:String,tableName:String): Unit ={
    val properties = new Properties()
    properties.setProperty("driver","com.mysql.jdbc.Driver")
    dataFrame.write.mode(SaveMode.Overwrite).jdbc(writeConnection,tableName,properties)
  }

}
