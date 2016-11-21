package com.kunyan.userportrait

import com.ibm.icu.text.CharsetDetector
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}


/**
  * Created by yangshuai on 2016/3/16.
  */
object Test extends App {

  var hbaseConf = HBaseConfiguration.create

  hbaseConf.set("hbase.rootdir", "hdfs:192.168.1.89:9000/hbase")
  hbaseConf.set("hbase.zookeeper.quorum", "192.168.1.89")

  val connection = ConnectionFactory.createConnection(hbaseConf)
  connection.getAdmin.listTableNames().foreach(x => {println(x.getNameAsString)})

  val table = connection.getTable(TableName.valueOf(args(0)))
//  val get = new Get("007e7a92d3f5879749eaa92faf5adfe0".getBytes)
//  val result = table.get(get)
//  val body = result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("content"))
//  val url = result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("url"))



//  val charSet = getCharsetName(body)

//  val doc = Jsoup.parse(new String(body, charSet))
//  println(doc)
/*  val lis =  doc.getElementById("newsListContent").getElementsByTag("li")

  for (i <- 0 until lis.size()) {

    val li = lis.get(i)
    val a = li.getElementsByTag("a").get(0)
    println(a.text())

  }*/





  val scan = new Scan
  scan.setMaxVersions()
  scan.setTimeRange(1448899200000l, 1462032000000l)
  val scanner = table.getScanner(scan)
  var counter = 0
  val iterator = scanner.iterator
  while (iterator.hasNext) {
    val row = iterator.next()
    val key = row.getRow
    val delete = new Delete(key)
    table.delete(delete)
    counter += 1
    if (counter % 10000 == 0) {
      println(counter)
      println(new String(key, "utf-8"))
    }
  }


  connection.close()

  def getCharsetName(html: Array[Byte]): String = {

    val icu4j = new CharsetDetector()
    icu4j.setText(html)
    val encoding = icu4j.detect()

    encoding.getName
  }

}
