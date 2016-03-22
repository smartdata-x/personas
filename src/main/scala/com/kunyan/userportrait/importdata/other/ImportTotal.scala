package com.kunyan.userportrait.importdata.other

import java.sql.DriverManager

import com.kunyan.userportrait.config.SparkConfig
import org.apache.log4j.Logger
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yangshuai on 2016/3/2.
  */
object ImportTotal {

  val url = "jdbc:mysql://:3306/personas"
  val driver = "com.mysql.jdbc.Driver"
  val username = "root"
  val password = "hadoop"
  Class.forName(driver)
  val connection = DriverManager.getConnection(url, username, password)
  connection.setAutoCommit(false)


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("USER PORTRAIT").set("spark.serializer",SparkConfig.SPARK_SERIALIZER).set("spark.kryoserializer.buffer.max",SparkConfig.SPARK_KRYOSERIALIZER_BUFFER_MAX)
    val sc = new SparkContext(sparkConf)

    val insertStr = "insert into total (ad, ua, mail, phone, qq, weibo) values (?, ?, ?, ?, ?, ?)"

    val ps = connection.prepareStatement(insertStr)

    var i = 0

    val input = sc.textFile(args(0)).persist(StorageLevel.MEMORY_AND_DISK)

    Logger.getRootLogger.warn(input.count())

    input.collect
      .map(_.split("\t"))
      .filter(_.length == 6)
      .foreach(arr => {

        if (arr(2).length < 500 && arr(3).length < 500 && arr(4).length < 500 && arr(5).length < 500) {

          ps.setString(1, arr(0))
          ps.setString(2, arr(1))
          ps.setString(3, arr(2))
          ps.setString(4, arr(3))
          ps.setString(5, arr(4))
          ps.setString(6, arr(5))

          ps.addBatch()

          i += 1
          if (i % 1000 == 0)
            ps.executeBatch()

        }

      })

    ps.executeBatch()
    connection.commit()
    connection.close()

    sc.stop()
  }

}
