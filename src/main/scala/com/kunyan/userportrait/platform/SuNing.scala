package com.kunyan.userportrait.platform

import scala.collection.mutable.ListBuffer

/**
  * Created by C.J.YOU on 2016/2/24.
  */
object SuNing extends  Platform {
  override val TOP_LEVEL_DOMAIN: String = "%.suning.com%"
  override val PLATFORM_NAME: String = "SuNing"
  val urlListBuffer = new ListBuffer[String]
  //  个人信息
  def extractPersonalInfo(url:String): Boolean ={
    var flag = false
    val templet = "http://my.suning.com/person.do"
    if(url.contains(templet)){
      urlListBuffer.+=(url)
      flag  = true
    }
    flag
  }
  // address
  def exractAddress(url:String): Boolean ={
    var flag = false
    val templet = "http://my.suning.com/address.do"
    if(url.contains(templet)){
      urlListBuffer.+=(url)
      flag  = true
    }
    flag
  }

  def extract(url:String): Unit ={
    if(extractPersonalInfo(url)){
      ;
    }else{
      exractAddress(url)
    }
  }
}
