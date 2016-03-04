package com.kunyan.userportrait.rule.url

import com.kunyan.userportrait.util.StringUtil

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


/**
  * Created by C.J.YOU on 2016/2/24.
  */
object WeiBo extends  Platform {

  override val PLATFORM_NAME_INFO: String = "WeiBo"
  override val TOP_LEVEL_DOMAIN: String = "%weibo.com%"
  override val PLATFORM_NAME_HTTP: String = "WeiBoHttp"

  //个人信息
  def PersonInformation(url:String):Boolean={
    url.contains("(?<=http://weibo.com/p/")
  }

  def getWeiBoIdFromCookie(cookie:String): String ={
    var weiBoId = ""
    val weiBoIdSet = new mutable.HashSet[String]()
    if (cookie != "NoDef") {
      val result = StringUtil.decodeBase64(cookie)
      // val result = cookie
      val template = "(?<=SUS=SID-)\\d{1,}(?=-)".r
      val resultWeiBoId  =  template.findAllMatchIn(result)
      resultWeiBoId.foreach(x => {
        weiBoId = x.toString()
      })
      if(!"".equals(weiBoId)){
        weiBoIdSet.+=(weiBoId)
      }
      val wbFeedUnfolded = "(?<=wb_feed_unfolded_)\\d{1,}(?==)".r
      val resultWeiBoId2  =  wbFeedUnfolded.findAllMatchIn(result)
      resultWeiBoId2.foreach(x => {
        weiBoId = x.toString()
      })
      if(!"".equals(weiBoId)){
        weiBoIdSet.+=(weiBoId)
      }
    }
    weiBoId = weiBoIdSet.mkString(",").toString
    weiBoId
  }

  def getEmailFromCookie(cookie:String): String ={
    var email = ""
    val emailSet = new mutable.HashSet[String]()
    if (cookie != "NoDef") {
      val result = StringUtil.decodeBase64 (cookie)
      // val result = cookie
      val template = "(?<=un=)\\w+@\\w+\\.com(?=;)".r
      val resultEmail = template.findAllMatchIn (result)
      resultEmail.foreach (x => {
        email = x.toString
      })
      if (!"".equals (email)) {
        emailSet.+= (email)
      }
    }
    email = emailSet.mkString (",").toString
    // println("email:"+email)
    email
  }

  def getWeiBoId(line:String): String ={
    val  lineSplit = line.split("\t")
    val cookies = lineSplit(3)
    val cookiesWeiBo = getWeiBoIdFromCookie(cookies)
    cookiesWeiBo
  }

  def getEmail(line:String): String = {
    val  lineSplit = line.split("\t")
    val cookies = lineSplit(3)
    val cookiesEmail = getEmailFromCookie(cookies)
    cookiesEmail
  }

  def getPhoneFromCookies(cookies:String):mutable.HashSet[String] = {
    var phone = ""
    val phoneSet = new mutable.HashSet[String]()
    if (cookies != "NoDef") {
      val result = StringUtil.decodeBase64(cookies)
      val template = "(?<=cSaveState=)\\d{11}".r
      val resultPhone  =  template.findAllMatchIn(result)
      resultPhone.foreach(x => {
        phone = x.toString()
      })
      phoneSet.+=(phone)

      val resultUser = StringUtil.decodeBase64(cookies)
      val templateUser = "(?<=un=)\\d{11}".r
      val userPhone  =  templateUser.findAllMatchIn(resultUser)
      userPhone.foreach(x => {
        phone = x.toString()
      })
      phoneSet.+=(phone)
    }
    phoneSet
  }

  def getPhone(line:String): mutable.HashSet[String] ={
    val  lineSplit = line.split("\t")
    val cookies = lineSplit(3)
    val cookiesPhone = getPhoneFromCookies(cookies)
    cookiesPhone
  }

  def extractUrl(url:String):String ={
    if(PersonInformation(url)) url else "NoDef"
  }

  def getWeiBoUrl(line:String):String ={
    val  url = line.split("\t")(2)
    // println(extractUrl(url))
    extractUrl(url)
  }

  // get user info
  def getUserInfo(cookieValue:String):ListBuffer[String]={

    val infoList = new ListBuffer[String]
    val un = cookieValue.indexOf("un=")
    if(un != -1){
      val d = cookieValue.indexOf(";",cookieValue.indexOf("un="))
      val b = cookieValue.substring(un+3,d)
      infoList.+=(b)
    }
    val cSaveStateStart = cookieValue.indexOf("cSaveState=")

    if(cSaveStateStart != -1){
      val cSaveStateEnd = cookieValue.indexOf(";",cookieValue.indexOf("cSaveState="))
      val phone = cookieValue.substring(cSaveStateStart+11,cSaveStateEnd)
      infoList.+=(phone)
    }
    infoList.+=("18817511172")
    infoList
  }

}
