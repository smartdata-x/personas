package com.kunyan.userportrait.util

import java.io._

import com.kunyan.userportrait.config.FileConfig
import com.kunyan.userportrait.platform.{PlatformScheduler, Eleme}

import scala.collection.mutable
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
    * @author C.J.YOU
    * @param array
    * @param pType
    */
  def saveAdAndUaAndUrl(array:Array[String], pType:Int): Unit ={
    val dateStr = TimeUtil.getDay
    val destPath = FileConfig.USER_DATA + "/" +PlatformScheduler.apply(pType).PLATFORM_NAME
    val dateDir = destPath + "/" + dateStr
    mkDir(dateDir)
    createFile(dateDir +"/"+TimeUtil.getCurrentHour,array)
  }

}

