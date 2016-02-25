package com.kunyan.userportrait.platform

import scala.collection.mutable.ListBuffer

/**
  * Created by C.J.YOU on 2016/2/24.
  */
object Eleme extends  Platform{

  override val TOP_LEVEL_DOMAIN = "%www.ele.me%"
  override val PLATFORM_NAME = "Eleme"
  val urlListBuffer = new ListBuffer[String]()
  // home
  def extractHmomePage(url:String): Boolean ={
    var flag = false
    val templet = "http://www.ele.me/"
    if(url == templet){
      urlListBuffer.+=(url)
      flag = true
    }
    flag
  }

  // shop
  def extractShop(url:String): Boolean ={
    var flag = false
    val templet = "http://www.ele.me/shop/\\d{1,}".r
    val result  = templet.findAllMatchIn(url)
    result.foreach(x => {
      urlListBuffer.+=(url)
      flag = true
      // println(url)
    })
    flag
  }

  // personalInfo
  def personalInfo(url:String): Boolean ={
    var flag = false
    val templet = "https://www.ele.me/profile/info"
    if(url.contains(templet)){
      urlListBuffer.+=(url)
      flag = true

    }
    flag
  }

  // address
  def extractAddress(url:String): Boolean ={
    var flag = false
    val templet = "http://www.ele.me/profile/address"
    if(url.contains(templet)){
      urlListBuffer.+=(url)
      flag  = true
    }
    flag
  }
  def extract(url:String): Unit ={
    if(extractHmomePage(url)){
      ;
    }else if(extractShop(url)){
      ;
    }else{
      extractAddress(url)
    }
  }
}
