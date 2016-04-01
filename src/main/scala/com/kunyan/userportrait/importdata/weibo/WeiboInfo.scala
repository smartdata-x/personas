package com.kunyan.userportrait.importdata.weibo

import com.kunyan.userportrait.config.SparkConfig
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Scan, ConnectionFactory}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkContext, SparkConf}
import org.jsoup.Jsoup

/**
  * Created by yangshuai on 2016/3/25.
  */
object WeiboInfo {

  def main(args: Array[String]): Unit = {

/*    val sparkConf = new SparkConf().setAppName("USER PORTRAIT")
      .set("spark.serializer", SparkConfig.SPARK_SERIALIZER)
      .set("spark.kryoserializer.buffer.max", SparkConfig.SPARK_KRYOSERIALIZER_BUFFER_MAX)
    val sc = new SparkContext(sparkConf)*/

    val hbaseConf = HBaseConfiguration.create

    hbaseConf.set("hbase.rootdir", "hdfs://localhost:9000/hbase")
    hbaseConf.set("hbase.zookeeper.quorum", "server0,server1,server2")

    //设置查询的表名
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "weibo_info")

    /*val usersRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    //遍历输出
    usersRDD.map { case (_, result) =>

      var value = ""

      val ad = Bytes.toString(result.getValue("info".getBytes, "ad".getBytes))
      val content = Bytes.toString(result.getValue("info".getBytes, "content".getBytes))

      val doc = Jsoup.parse(content)
      val name = doc.getElementsByAttributeValueContaining("node-type", "nickname_view").text()
      if (!name.contains("马上填写")) {
        value = name
      }

      (ad, value)
    }.filter(_._2.nonEmpty).map(x => x._1 + "\t" + x._2).saveAsTextFile(args(0))

    sc.stop()*/

    val connection = ConnectionFactory.createConnection(hbaseConf)
    val table = connection.getTable(TableName.valueOf("weibo_info"))
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("content"))

    val scanner = table.getScanner(scan)

    var result = scanner.next()

    var count = 0

    while (result != null) {
      count += 1
      val html = new String(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("content")))
      val doc = Jsoup.parse(html)
//      val value = doc.getElementsByAttributeValueContaining("node-type", "nickname_view").text
//      if (value.trim().nonEmpty && !value.contains("马上填写"))
//        println(value)
      println(doc)
      result = scanner.next()

      if (count % 500 == 0)
        println(count)
    }

  }

}
