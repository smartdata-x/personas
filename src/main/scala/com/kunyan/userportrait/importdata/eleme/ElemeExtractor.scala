package com.kunyan.userportrait.importdata.eleme

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSON

/**
  * Created by yangshuai on 2016/3/12.
  */
object ElemeExtractor {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("USER PORTRAIT")
    val sc = new SparkContext(sparkConf)

    val hbaseConf = HBaseConfiguration.create

    hbaseConf.set("hbase.rootdir", "hdfs://:9000/hbase")
    hbaseConf.set("hbase.zookeeper.quorum", "server")

    //设置查询的表名
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "eleme")

    val usersRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    //遍历输出
    val results = usersRDD.map{ case (_,result) =>
      val ad = Bytes.toString(result.getValue("info".getBytes,"ad".getBytes))
      val ua = Bytes.toString(result.getValue("info".getBytes,"ua".getBytes))
      val content = Bytes.toString(result.getValue("info".getBytes,"content".getBytes))

      var list = List[Map[String, String]]()

      JSON.parseFull(content) match {
        case None =>
        case _ =>
        if (!content.startsWith("[")) {
          JSON.parseFull(content).get.asInstanceOf[Map[String, Any]].get("addresses") match {
            case None =>
            case _ =>
            list = JSON.parseFull(content).get.asInstanceOf[Map[String, Any]].get("addresses").get.asInstanceOf[List[Map[String, String]]]
          }
        } else {
          list = JSON.parseFull(content).get.asInstanceOf[List[Map[String, String]]]
        }
      }

      (ad, ua, list)
    }.flatMap(tuple => {
      val list = ListBuffer[Array[String]]()
      tuple._3.foreach(x => {
        val name = x.getOrElse("name", "")
        val address = x.getOrElse("address", "") + "_" + x.getOrElse("address_detail", "")
        val phone = x.getOrElse("phone", "")
        list += Array[String](tuple._1, tuple._2, name, address, phone)
      })

      list
    })

    results.persist(StorageLevel.MEMORY_AND_DISK)

    results.map(x => {
      x(0) + "\t" + x(1) + "\t" + x(2)
    }).saveAsTextFile(args(1) + "_name")

    results.map(x => {
      x(0) + "\t" + x(1) + "\t" + x(3)
    }).saveAsTextFile(args(1) + "_address")

    results.map(x => {
      x(0) + "\t" + x(1) + "\t" + x(4)
    }).saveAsTextFile(args(1) + "_phone")

    sc.stop()
  }

  def toJson(arr: Array[String]): Array[String] = {
    val str1 = String.format("{\"ad\":\"%s\", \"ua\":\"%s\", \"url\":\"%s\", \"cookie\":\"%s\", \"platform\":\"eleme_info\"}", arr(0), arr(1), "https://www.ele.me/profile/info", arr(3))
    val str2 = String.format("{\"ad\":\"%s\", \"ua\":\"%s\", \"url\":\"%s\", \"cookie\":\"%s\", \"platform\":\"eleme_address\"}", arr(0), arr(1), "https://www.ele.me/profile/address", arr(3))
    Array[String](str1, str2)
  }
}
