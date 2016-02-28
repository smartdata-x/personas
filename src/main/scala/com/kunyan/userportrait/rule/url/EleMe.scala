package com.kunyan.userportrait.rule.url

import scala.collection.mutable.ListBuffer

/**
  * Created by C.J.YOU on 2016/2/24.
  */
object EleMe extends  Platform{

  override val TOP_LEVEL_DOMAIN = "%www.ele.me%"
  override val PLATFORM_NAME_INFO = "Eleme"
  override val PLATFORM_NAME_HTTP: String = "ElemeHttp"
  val urlListBuffer = new ListBuffer[String]()

  // home
  def extractHomePage(url:String): Boolean ={
    var flag = false
    if(url == "http://www.ele.me/"){
      flag = true
    }
    flag
  }

  // shop
  def extractShop(url:String): Boolean ={
    var flag = false
    val template = "http://www.ele.me/shop/\\d{1,}".r
    val result  = template.findAllMatchIn(url)
    result.foreach(x => {
      flag = true
    })
    flag
  }

  // personalInfo
  def personalInfo(url:String): Boolean ={
    var flag = false
    if(url.contains("https://www.ele.me/profile/info")){
      flag = true
    }
    flag
  }

  // address
  def extractAddress(url:String): Boolean ={
    var flag = false
    if(url.contains("http://www.ele.me/profile/address")){
      flag  = true
    }
    flag
  }

  def extract(line:String): Unit ={
    val url = line.split("\t")(2)
    if(extractHomePage(url)){
      urlListBuffer.+=(line)
    }else if(extractShop(url)){
      urlListBuffer.+=(line)
    }else if(extractAddress(url)){
      urlListBuffer.+=(line)
    }
  }

}
