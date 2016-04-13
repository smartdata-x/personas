package com.kunyan.userportrait.db

import com.kunyan.userportrait.log.PLogger
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import scala.util.parsing.json.JSON

/**
  * Created by yangshuai on 2016/2/29.
  */
object HbaseUtil {

  var hbaseConf = HBaseConfiguration.create

/*  hbaseConf.set("hbase.rootdir", "hdfs:222.73.34.99:9000/hbase")
  hbaseConf.set("hbase.zookeeper.quorum", "server0,server1,server2")*/

  hbaseConf.set("hbase.rootdir", "hdfs:localhost:9000/hbase")
  hbaseConf.set("hbase.zookeeper.quorum", "master,slave1,slave2")

  private val connection = ConnectionFactory.createConnection(hbaseConf)

  def getTableByName(tableName: String): Table = {
    connection.getTable(TableName.valueOf(tableName))
  }

  /**
    * 判断hbase表中是否存在此数据
    *
    * @author sijiansheng
    **/
  def existRowkey(rowkey: String, hTable: Table): Boolean = {

    val get = new Get(rowkey.getBytes())
    val result = hTable.get(get)

    if (result.isEmpty) {
      return false
    }
    true
  }

  /**
    * 直接调用客户端从hbase中取数据
    **/
  def getDataByRowkey(tableName: String, rowkey: String): Result = {

    try {
      if (!connection.getAdmin.tableExists(TableName.valueOf(tableName))) {
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    val hTable = connection.getTable(TableName.valueOf(tableName))


    if (!existRowkey(rowkey, hTable)) {
      PLogger.error("Rowkey: " + rowkey + " does not exist in table: " + tableName + ".")
      return null
    }

    val get = new Get(rowkey.getBytes)

    hTable.get(get)
  }

  def getDataBetween(tableName: String, start: Int, end: Int): ResultScanner = {

    try {
      if (!connection.getAdmin.tableExists(TableName.valueOf(tableName))) {
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    val hTable = connection.getTable(TableName.valueOf(tableName))

    val scan = new Scan(Bytes.toBytes(start), Bytes.toBytes(end))

    hTable.getScanner(scan)
  }

  def close(): Unit = {
    connection.close()
  }

}
