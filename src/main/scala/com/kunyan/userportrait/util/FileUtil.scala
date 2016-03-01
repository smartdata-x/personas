package com.kunyan.userportrait.util

import java.io._

import com.kunyan.userportrait.config.FileConfig
import com.kunyan.userportrait.rule.url.PlatformScheduler

import scala.collection.mutable.ListBuffer

/**
  * Created by C.J.YOU on 2016/2/24.
  * FS操作的工具类
  */
object FileUtil {

  /** 创建目录 */
  def mkDir(name: String): Boolean = {

    val dir = new File (name)
    dir.mkdir
  }

  def readFile(path: String): ListBuffer[String] = {

    var lines = new ListBuffer[String]()

    val br = new BufferedReader (new FileReader (path))

    try {
      var line = br.readLine ()

      while (line != null) {
        lines += line
        line = br.readLine ()
      }
      lines
    } finally {
      br.close ()
    }
  }

  def createFile(path: String,array:Array[String]): Unit = {

    val writer = new PrintWriter(path, "UTF-8")
    for(line <- array){
      writer.println(line)
    }
    writer.close()
  }

  /**
    * 保存AD,UA,URL
    */
  def saveAdAndUaAndUrl(array:Array[String], pType:Int,dType:Int): Unit ={
    val dateStr = TimeUtil.getDay
    var filter = array
    var destPath = ""
    if(dType == 1){
      destPath= FileConfig.USER_DATA + "/" +PlatformScheduler.apply(pType).PLATFORM_NAME_HTTP +"/" + dateStr
    }else if(dType == 2){
      destPath = FileConfig.USER_DATA + "/" +PlatformScheduler.apply(pType).PLATFORM_NAME_INFO +"/" + dateStr
      filter = array
    }
    mkDir(destPath)
    createFile(destPath +"/"+TimeUtil.getCurrentHour,filter)
  }

}

