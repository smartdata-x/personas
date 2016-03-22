package com.kunyan.userportrait.importdata.eleme

import java.sql.DriverManager

import scala.collection.mutable

/**
  * Created by yangshuai on 2016/3/1.
  * 整合ua相同的数据
  */
object CombineEleMe {

  val url = "jdbc:mysql://:3306/personas"
  val driver = "com.mysql.jdbc.Driver"
  val username = "root"
  val password = "hadoop"
  Class.forName(driver)
  val connection = DriverManager.getConnection(url, username, password)
  connection.setAutoCommit(false)

  var set = Set[String]()

  def main(args: Array[String]): Unit = {

    val selectCountStr = "select ua, count(ua) as c_u from temp group by ua order by c_u desc;"
    val statement = connection.createStatement()
    val rs = statement.executeQuery(selectCountStr)

    while (rs.next()) {

      val ua = rs.getString(1)
      val count = rs.getInt(2)

      if (count > 1)
        set += ua

    }

    set.foreach(x => {

      val selectMulti = String.format("select id, ad, ua, main_name, main_phone, main_address, all_name, all_phone from temp where ua=\"%s\"", x)
      val rs = statement.executeQuery(selectMulti)

      var ids = Set[Int]()

      val adMap = mutable.Map[String, Int]()
      val nameMap = mutable.Map[String, Int]()
      val phoneMap = mutable.Map[String, Int]()
      val addressMap = mutable.Map[String, Int]()

      var adSet = Set[String]()
      var nameSet = Set[String]()
      var phoneSet = Set[String]()
      var addressSet = Set[String]()

      while (rs.next()) {

        val id = rs.getInt(1)
        val ad = rs.getString(2)
        val ua = rs.getString(3)
        val mainName = rs.getString(4)
        val mainPhone = rs.getString(5)
        val mainAddress = rs.getString(6)
        val allName = rs.getString(7)
        val allPhone = rs.getString(8)

        ids += id

        adMap.put(ad, adMap.getOrElse(ad, 0) + 1)
        nameMap.put(mainName, nameMap.getOrElse(ad, 0) + 1)
        phoneMap.put(mainPhone, phoneMap.getOrElse(ad, 0) + 1)
        addressMap.put(mainAddress, addressMap.getOrElse(ad, 0) + 1)

        adSet += ad
        nameSet ++= allName.split(",")
        phoneSet ++= allPhone.split(",")
        addressSet += mainAddress

      }

      println(ids)

      println(getKeyWithMaxValue(adMap))
      println(getKeyWithMaxValue(nameMap))
      println(getKeyWithMaxValue(phoneMap))
      println(getKeyWithMaxValue(addressMap))

      println(nameSet)
      println(phoneSet)
      println(addressSet)

      try {
        val insertStr = "insert into temp (ad, ua, main_name, main_phone, main_address, all_ad, all_name, all_phone, all_address) values (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        var ps = connection.prepareStatement(insertStr)
        ps.setString(1, getKeyWithMaxValue(adMap))
        ps.setString(2, x)
        ps.setString(3, getKeyWithMaxValue(nameMap))
        ps.setString(4, getKeyWithMaxValue(phoneMap))
        ps.setString(5, getKeyWithMaxValue(addressMap))
        ps.setString(6, adSet.mkString("-->>"))
        ps.setString(7, nameSet.mkString("-->>"))
        ps.setString(8, phoneSet.mkString("-->>"))
        ps.setString(9, addressSet.mkString("-->>"))
        ps.addBatch()
        ps.executeBatch()

        ids.foreach(id => {
          val deleteStr = "delete from temp where id=?"
          println(deleteStr + id)
          ps = connection.prepareStatement(deleteStr)
          ps.setInt(1, id)
          ps.addBatch()
          ps.executeBatch()
        })

        connection.commit()

      } catch {
        case e:Exception =>
          println(e)
          connection.rollback()
      }

    })

  }

  def getKeyWithMaxValue(map: mutable.Map[String, Int]): String = {
    var max = 0
    var resultKey = ""
    map.foreach(x => {
      if (x._2 > max) {
        max = x._2
        resultKey = x._1
      }
    })

    resultKey
  }
}
