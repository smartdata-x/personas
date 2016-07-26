/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /home/wukun/SparkKafka/src/main/scala/XmlHandle.scala
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-04-11 10:27
#    Description  : 
=============================================================================*/
package com.kunyan.userportrait.dynamiclable

import scala.xml._

/**
  * Created by wukun on 2016/5/23
  * xml操作句柄
  */
class XmlHandle private(val xmlPath: String) extends Serializable {

  val xmlConfig = loadXml()

  def loadXml(): Elem = {
    XML.loadFile(xmlPath)
  }

  def getElem(elemName: String): String = {
    (xmlConfig\elemName).text
  }
  
  def getElem(firstName: String, secondName: String): String = {
    (xmlConfig\firstName\secondName).text
  }

  override def toString(): String = {
    this.xmlPath
  }
}

/**
  * Created by wukun on 2016/5/23
  * xml操作句柄伴生对象
  */
object XmlHandle {

  var xmlHandle: XmlHandle = _

  /**
    * 获取全局唯一的操作句柄实例
    * @param xmlPath 路径
    * @author wukun
    */
  def apply(xmlPath: String): XmlHandle = {

    if(xmlHandle == null) {
      xmlHandle = new XmlHandle(xmlPath)
    }

    xmlHandle
  }
}


