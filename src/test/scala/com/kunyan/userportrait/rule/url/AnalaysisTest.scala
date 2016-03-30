package com.kunyan.userportrait.rule.url

import java.sql.DriverManager

import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable
import scala.util.parsing.json.JSON

/**
  * Created by C.J.YOU on 2016/2/25.
  */
class AnalaysisTest  extends FlatSpec with Matchers{

//  it should "work" in{
//    Analysis.loadData(Extractor.sc,"F:\\datatest\\data\\000095_0",FileFormatConfig.tableName)
//    Analysis.getAdAndUaAndUrl(PlatformConfig.PLATFORM_SUNING).foreach(SuNing.extract(_))
//    SuNing.urlListBuffer.foreach(x => println("suning:"+x))
//  }

//  it should "work " in {
//    Analysis.loadData(Extractor.sc,"F:\\datatest\\data\\000136_0",FileFormatConfig.tableName)
//    Analysis.getAdAndUa(PlatformConfig.PLATFORM_WEIBO)
//  }

//  it should "work" in{
//    Analysis.loadData(Extractor.sc,"F:\\datatest\\data\\000095_0",FileFormatConfig.tableName)
//    Analysis.getAdAndUaAndUrl(PlatformConfig.PLATFORM_ELEME).foreach(Eleme.extract(_))
//    Eleme.urlListBuffer.foreach(x => println("ELEME:"+x))
//  }

//  it should "work" in{
//    Analysis.loadData(Extractor.sc,"F:\\datatest\\data\\000095_0",FileFormatConfig.tableName)
//    Analysis.getAdAndUaAndUrl(PlatformConfig.PLATFORM_QZONE).foreach(Qzone.QQzone(_))
//    FileUtil.saveAdAndUaAndUrl(Qzone.urlListBuffer.distinct.toArray.++(Qzone.QQListBuffer.distinct) ,PlatformConfig.PLATFORM_QZONE)
//
//  }

//
//  it should "work" in{
//    Analysis.loadData(Extractor.sc,"F:\\datatest\\data\\000095_0",FileFormatConfig.tableName)
//    Analysis.getAdAndUaAndUrl(PlatformConfig.PLATFORM_WEIBO).foreach(WeiBo.WeiBo(_))
//    FileUtil.saveAdAndUaAndUrl(WeiBo.urlListBuffer.distinct.toArray,PlatformConfig.PLATFORM_WEIBO)
//  }

//  it should "work" in{
//    Analysis.loadData(Extractor.sc,"F:\\datatest\\data\\000095_0",FileFormatConfig.tableName)
//    Analysis.getAdAndUaAndUrl(PlatformConfig.PLATFORM_ZHIHU).foreach(ZhiHu.extract(_))
//    ZhiHu.urlListBuffer.foreach(x => println("ZhiHu:"+x))
//  }

  it should "" in {

    val url = "jdbc:mysql://222.73.34.104:3306/personas"
    val driver = "com.mysql.jdbc.Driver"
    val username = "root"
    val password = "hadoop"
    Class.forName(driver)
    val connection = DriverManager.getConnection(url, username, password)
    connection.setAutoCommit(false)

    val insertStr = "insert into main_index (ad, ua) values (?,?)"

    val ps = connection.prepareStatement(insertStr)

    val st = connection.createStatement()
    var id = -1

    var rs = st.executeQuery("select * from main_index where ad=\"a\" and ua=\"b\"")
    while (rs.next()) {
      id = rs.getInt(1)
    }

    if (id == -1) {
      ps.setString(1, "a")
      ps.setString(2, "b")
      ps.addBatch()
    }

    ps.executeBatch()
    connection.commit()

    rs = st.executeQuery("select * from main_index where ad=\"a\" and ua=\"b\"")
    while (rs.next()) {
      id = rs.getInt(1)
    }

    println(id)
    connection.close()
  }

}
