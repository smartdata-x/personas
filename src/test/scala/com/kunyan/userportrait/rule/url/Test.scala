package com.kunyan.userportrait.rule.url

import org.apache.hadoop.hbase.client.{ConnectionFactory, Get}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}


/**
  * Created by yangshuai on 2016/3/16.
  */
object Test extends App {

  var hbaseConf = HBaseConfiguration.create

  hbaseConf.set("hbase.rootdir", "hdfs:localhost:9000/hbase")
  hbaseConf.set("hbase.zookeeper.quorum", "master,slave1,slave2,slave3,slave4")

//  hbaseConf.set("hbase.rootdir", "hdfs:222.73.34.99:9000/hbase")
//    hbaseConf.set("hbase.zookeeper.quorum", "server0,server1,server2")

  val connection = ConnectionFactory.createConnection(hbaseConf)

  val hTable = connection.getTable(TableName.valueOf("20"))

  val get = new Get("ccf8333142812a36bbc94c1893d5fd57".getBytes)

  val result = hTable.get(get)

//  val content = new String(result.getValue("basic".getBytes, "content".getBytes))

//  println(content)

}
