package com.kunyan.userportrait.importdata.eleme

import java.sql.DriverManager

import com.kunyan.userportrait.db.HbaseUtil
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable
import scala.util.parsing.json.JSON

/**
  * Created by yangshuai on 2016/3/1.
  * 将饿了么数据导入mysql
  */
object ImportEleMe {

  val url = "jdbc:mysql://:3306/persona?useUnicode=true&characterEncoding=utf-8"
  val driver = "com.mysql.jdbc.Driver"
  val username = "root"
  val password = "dataservice2016"
  Class.forName(driver)
  val connection = DriverManager.getConnection(url, username, password)

  def main(args: Array[String]): Unit = {

    val set = mutable.Set[String]()

    val table = HbaseUtil.getTableByName("eleme_info")

    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("content"))

    val scanner = table.getScanner(scan)

    var list = List[Map[String, String]]()

    val insertStr = "insert into o2o_info (name, phone, address) values (?, ?, ?)"
    val ps = connection.prepareStatement(insertStr)
    connection.setAutoCommit(false)

    var count = 0

    try {

      var result = scanner.next()

      while (result != null) {

        val content = new String(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("content")))


        if (!content.startsWith("[")) {
          list = JSON.parseFull(content).get.asInstanceOf[Map[String, Any]].get("addresses").get.asInstanceOf[List[Map[String, String]]]
        } else {
          list = JSON.parseFull(content).get.asInstanceOf[List[Map[String, String]]]
        }

        if (list.nonEmpty)
          set ++= getItem(list)

        result = scanner.next()
      }

    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      scanner.close()
      HbaseUtil.close()
    }

    println(set.size)

    try {

      set.foreach(str => {
        val arr = str.split("\t")
        if (arr.length == 3) {

          count += 1

          ps.setString(1, arr(0))
          ps.setString(2, arr(1))
          ps.setString(3, arr(2))
          ps.addBatch()

          if (count % 1000 == 0) {
            println(count)
            ps.executeBatch()
            connection.commit()
          }
        }
      })

      ps.executeBatch()
      connection.commit()

    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      ps.close()
      connection.close()
    }

  }

  def getItem(dicts: List[Map[String, String]]): mutable.Set[String] = {

    val set = mutable.Set[String]()

    for (i <- dicts.indices) {

      val dict = dicts(i)

      val name = dict.getOrElse("name", "")
      val address = dict.getOrElse("address", "") + "_" + dict.getOrElse("address_detail", "")
      val phone = dict.getOrElse("phone", "")

      set += name + "\t" + phone + "\t" + address
    }

    set
  }

}
