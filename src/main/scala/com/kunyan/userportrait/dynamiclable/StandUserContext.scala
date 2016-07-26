/*********************************************************************************************
* Copyright @ 2015 ShanghaiKunyan. All rights reserved
* @filename : /opt/spark-1.2.2-bin-hadoop2.4/work/spark/SparkKafka/src/main/scala/StandUserContext.scala
* @author   : Sunsolo
* Email     : wukun@kunyan-inc.com
* Date      : 2016-07-25 16:10
***********************************************************************************************/
package com.kunyan.userportrait.dynamiclable

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import scala.io.Source
import scala.util.matching.Regex

/**
  * Created by wukun on 2016/7/25
  * 匹配标准用户上下文
  */
class StandUserContext(val xmlHandle: XmlHandle) {

  val (dataDays, dataHours, dayThreshold, weekThreshold, standUrl) = {
    init
  }

  private val pattern = """(http://|\b)(([^/]*)/|([^/]*)).*""".r

  private lazy val sc = new SparkContext(new SparkConf().setAppName("standUser"))

  /**
    * 初始化类数据成员
    * @return 匹配url的记录
    * @author wukun
    */
  def init(): (Int, Int, Int, Int, Array[String]) = {

    val days = xmlHandle.getElem("file", "days").toInt
    val hours = xmlHandle.getElem("file", "hours").toInt
    val dayThrd = xmlHandle.getElem("file", "daythrd").toInt
    val weekThrd = xmlHandle.getElem("file", "weekthrd").toInt
    val urlPath = xmlHandle.getElem("file", "path")

    val content = Source.fromFile(urlPath).getLines().toArray
    val urlSet = content(0).split(",")

    (days, hours, dayThrd, weekThrd, urlSet)
  }

  def generateRdd(dayTh: Int, hourTh: Int): RDD[String]= {

    val fileName = "/user/wukun/train/2016-07-1" + dayTh + "/" + hourTh

    sc.textFile(fileName).distinct
  }

}

/**
  * Created by wukun on 2016/7/25
  * 匹配标准用户上下文伴生类
  */
object StandUserContext {

  var suc: StandUserContext = _

  /**
    * 获取全局唯一的上下文实例
    * @param xmlHandle xml操作句柄
    * @author wukun
    */
  def apply(xmlHandle: XmlHandle): StandUserContext = {

    if(suc == null) {
      suc = new StandUserContext(xmlHandle)
    }

    suc
  }

  def main(args: Array[String]) {

    val xmlHandle = XmlHandle("./config.xml")

    val suc = StandUserContext(xmlHandle)

    println(suc.dataDays + ":" + suc.dataHours + ":" + suc.dayThreshold + ":" + suc.weekThreshold)
  }
}



