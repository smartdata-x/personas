/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /home/wukun/work/SparkKafka/src/main/scala/ObtainExtUrl.scala
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-07-26 07:00
=============================================================================*/
package com.kunyan.userportrait.dynamiclable

import UrlContext._
import FileHandle._

import java.io.File 

/**
  * Created by wukun on 2016/07/26
  * 获取扩充URL
  */
object ObtainExtUrl {

  def main(args: Array[String]) {

    val fileBuffer = (new File(args(0))).listFiles

    val urlContext = UrlContext(fileBuffer)
    urlContext.initUrlBuffer

    var extendUrl = ""
    urlContext.removeUrl.foreach( x => {
      extendUrl = extendUrl + "," + x
    })

    val fileHandle = FileHandle(args(1))
    val reader = fileHandle.initBuff
    var content = ""
    var line = ""

    while({

      line = reader.readLine
      line 

    } != null) {
      content += line
    }

    reader.close

    fileHandle.setPath(args(2))
    val writer = fileHandle.initWriter()
    content += extendUrl
    writer.write(content.trim)
    writer.close
  } 
}

