/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /home/wukun/work/SparkKafka/src/main/scala/UrlContext.scala
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-07-26 09:02
#    Description  : 
=============================================================================*/
package com.kunyan.userportrait.dynamiclable

import FileHandle._

import java.io.File 
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by wukun on 2016/07/26
  * 获取扩充URL上下文类
  */
class UrlContext(val listFile: Array[File]) { self =>

  type ListSet = mutable.ListBuffer[mutable.TreeSet[String]] 

  private lazy val urlBuffer = createUrlBuffer

  def createUrlBuffer: ListSet = {
    new ListSet
  }

  /**
    * 将文件中待扩展url读入TreeSet中
    * @param path 存放url的文件路径
    * @return 存放url存储结构体
    * @author wukun
    */
  def obtainUrlSet(path: String): mutable.TreeSet[String] = {

    val urlTree = new mutable.TreeSet[String]
    val fileHandle = FileHandle(path)
    val reader = fileHandle.initBuff

    var line: String = null
    var i = 1

    while(({

      line = reader.readLine
      line

    } != null) && i <= 100) {

      urlTree += (line.split(" "))(0)
      i = i + 1

    }

    reader.close 

    urlTree
  } 

  def initUrlBuffer {

    listFile.foreach( x => {
      urlBuffer += obtainUrlSet(x.getAbsolutePath)
    })
  }

  def getUrlBuffer: ListSet = {
    self.urlBuffer
  }

  /**
    * 在存放TreeSet的列表中筛选都包含的url 
    * @return 筛选出的url 
    * @author wukun
    */
  def removeUrl: ListBuffer[String] = {

    val headList = urlBuffer.head
    val tailList = urlBuffer.tail
    var isAllContain = true

    val urlList = new ListBuffer[String]

    headList.foreach( x => {

      tailList.foreach( y => {

        if(y.contains(x) == false) {
          isAllContain = false
        }

      })
      
      if(isAllContain == true) {
        urlList += x
      } else {
        isAllContain = true
      }

    })

    return urlList
  }
}

/**
  * Created by wukun on 2016/07/26
  * 获取扩充URL上下文类的伴生类
  */
object UrlContext {

  def apply(listFile: Array[File]): UrlContext = {
    new UrlContext(listFile)
  }
}

