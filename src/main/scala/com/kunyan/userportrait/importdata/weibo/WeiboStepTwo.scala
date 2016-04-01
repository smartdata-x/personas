package com.kunyan.userportrait.importdata.weibo

import com.kunyan.userportrait.config.SparkConfig
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

/**
  * Created by yangshuai on 2016/3/22.
  */
object WeiboStepTwo {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("USER PORTRAIT")
      .set("spark.serializer",SparkConfig.SPARK_SERIALIZER)
      .set("spark.kryoserializer.buffer.max",SparkConfig.SPARK_KRYOSERIALIZER_BUFFER_MAX)
    val sc = new SparkContext(sparkConf)

    val hbaseConf = HBaseConfiguration.create

    hbaseConf.set("hbase.rootdir", "hdfs://localhost:9000/hbase")
    hbaseConf.set("hbase.zookeeper.quorum", "server")

    //设置查询的表名
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "weibo_step1")

    val usersRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    //遍历输出
    usersRDD.map{ case (_,result) =>

      val ad = Bytes.toString(result.getValue("info".getBytes,"ad".getBytes))
      val ua = Bytes.toString(result.getValue("info".getBytes,"ua".getBytes))
      val content = Bytes.toString(result.getValue("info".getBytes,"content".getBytes))
      val cookie = Bytes.toString(result.getValue("info".getBytes, "cookie".getBytes))

      var url = ""

      if (content.contains("$CONFIG['page_id']='")) {
        val arr = content.split("CONFIG\\['page_id'\\]='")
        if (arr.length > 1) {
          val result = arr(1).split("';")(0)
          url = s"http://weibo.com/p/$result/info"
        }
      }

      (ad, ua, url, cookie)
    }.filter(_._3.nonEmpty).map(toJson).saveAsTextFile(args(0))

    sc.stop()
  }

  def toJson(tuple: (String, String, String, String)): String = {
    String.format("{\"ad\":\"%s\", \"ua\":\"%s\", \"url\":\"%s\", \"cookie\":\"http://account.weibo.com/set/iframe\", \"platform\":\"weibo_info\"}", tuple._1, tuple._2, tuple._3)
  }

  def toJson(tuple: (String, String, String)): String = {
    String.format("{\"ad\":\"%s\", \"ua\":\"%s\", \"url\":\"http://account.weibo.com/set/iframe\", \"cookie\":\"%s\", \"platform\":\"weibo_info\"}", tuple._1, tuple._2, tuple._3)
  }

  def toJson(arr: Array[String]): String = {
    String.format("{\"ad\":\"%s\", \"ua\":\"%s\", \"url\":\"http://account.weibo.com/set/iframe\", \"cookie\":\"%s\", \"platform\":\"weibo_info\"}", arr(0), arr(1), arr(3))
  }

/*  def convertJson(json: String): String = {
    val map = JSON.parseFull(json).get.asInstanceOf[Map[String, String]]
  }*/

}
