package com.kunyan.userportrait.rule.url

/**
  * Created by C.J.YOU on 2016/2/24.
  */
object EleMe extends  Platform{

  override val TOP_LEVEL_DOMAIN = "%www.ele.me%"
  override val PLATFORM_NAME_INFO = "Eleme"
  override val PLATFORM_NAME_HTTP: String = "ElemeHttp"

  // home
  def extractHomePage(url:String): Boolean ={
    url == "http://www.ele.me/"
  }

  // personalInfo:用户名，手机号，邮箱
  def personalInfo(url:String): Boolean ={
    url.contains("https://www.ele.me/profile/info")
  }

  // address:地址
  def extractAddress(url:String): Boolean ={
    url.contains("http://www.ele.me/profile/address")
  }

  def extractUrl(url:String): String ={
    if(extractHomePage(url) || extractAddress(url) || personalInfo(url)) url else "NoDef"
  }

  def getUrl(line:String):String ={
    val  url = line.split("\t")(2)
    // println(extractUrl(url))
    extractUrl(url)
  }

  // phone
  def phone(url:String):String ={
    //var phone = "15921452710"
    val phone = ""
    phone
  }


}
