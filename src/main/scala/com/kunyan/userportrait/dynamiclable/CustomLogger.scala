/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /home/wukun/work/Wokong/src/main/scala/com/kunyan/wokongsvc/realtimedata/WarnLogger.scala
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-05-25 17:58
#    Description  : 
=============================================================================*/
package com.kunyan.userportrait.dynamiclable

import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
  * Created by wukun on 2016/5/25
  * 打log日志的类需要继承此类
  */
trait CustomLogger extends Serializable {

  val logger = LoggerFactory.getLogger(classOf[CustomLogger])

  def warnLog(
    info: (String, String), 
    msg: String) {

    logger.warn("{}[{}]：{}", info._1, info._2, msg)
  }

  def errorLog(
    info: (String, String), 
    msg: String) {

    logger.error("{}[{}]：{}", info._1, info._2, msg)
  }

  /**
   * 获取日志所在的文件信息
   * @author wukun
   */
  def fileInfo: (String, String) = {
    (Thread.currentThread.getStackTrace()(2).getFileName,
      Thread.currentThread.getStackTrace()(2).getLineNumber.toString)
  }
}

