package com.kunyan.userportrait.platform

import scala.collection.mutable.ListBuffer

/**
  * Created by C.J.YOU on 2016/2/24.
  */
object WeiBo extends  Platform {
  override val PLATFORM_NAME_INFO: String = "WeiBo"
  override val TOP_LEVEL_DOMAIN: String = "%weibo.com%"
  override val PLATFORM_NAME_HTTP: String = "WeiBoHttp"

  val urlListBuffer = new ListBuffer[String]()
  val WeiBoListBuffer = new ListBuffer[String]()
  var QQ = new String
  //主页
  def HomePage(url:String):Boolean={
    var flag = false
    val homepage = "http://weibo.com/\\d{1,}/profile?".r
    val result  = homepage.findAllMatchIn(url)
    result.foreach(x => {
      flag =true
    })
    flag
  }
  //相册
  def PhotoAlbum(url:String):Boolean={
    var flag = false
    val photoalbum = "http://weibo.com/p/\\d{1,}/album?".r
    val result  = photoalbum.findAllMatchIn(url)
    result.foreach(x => {
      flag =true
    })
    flag
  }
  //管理
  def Manage(url:String):Boolean={
    var flag = false
    val manage = "http://weibo.com/p/\\d{1,}/manage?".r
    val result  = manage.findAllMatchIn(url)
    result.foreach(x => {
      flag =true
    })
    flag
  }
  //首页
  def Home(url:String):Boolean={
    var flag = false
    val home = "http://weibo.com/p/\\d{1,}/manage?".r
    val result  =home.findAllMatchIn(url)
    result.foreach(x => {
      flag =true
    })
    flag
  }
  //发现
  def Found(url:String):Boolean={
    var flag = false
    val found = "http://d.weibo.com/\\d{6}".r
    val result  =found.findAllMatchIn(url)
    result.foreach(x => {
      flag =true
    })
    flag
  }
  //个人信息
  def PersonInformation(url:String):Boolean={
    var flag = false
    val personInformation = "http://weibo.com/p/\\d{6}/nfo?mod=pedit".r
    val result  =personInformation.findAllMatchIn(url)
    result.foreach(x => {
      flag =true
    })
    flag
  }

  //QQ号匹配
  def QQ(url:String):Boolean={
    var flag = false
    val personInformation = "(?<=from=)\\d{8,}(?=)".r
    val result  = personInformation.findAllMatchIn(url)
    result.foreach(x => {
      flag =true
      QQ = result.toString()
    })
    flag
  }
  def WeiBo(line:String): Unit ={
    val  lineSplit = line.split("\t")
    val ad = lineSplit(0)
    val ua = lineSplit(1)
    val url = lineSplit(2)
    if(HomePage(url)){
      urlListBuffer.+=(line)
    }else if(PhotoAlbum(url)){
      urlListBuffer.+=(line)
    }else if(Manage(url)){
      urlListBuffer.+=(line)
    }else if(Home(url)){
      urlListBuffer.+=(line)
    }else if(Found(url)){
      urlListBuffer.+=(line)
    }else if(PersonInformation(url)){
      urlListBuffer.+=(line)
    }
    else if(PersonInformation(url)){
      urlListBuffer.+=(line)
      WeiBoListBuffer. += (ad +"\t"+ua+"\t"+QQ)
    }
  }


}
