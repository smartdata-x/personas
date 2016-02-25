package com.kunyan.userportrait.platform

import scala.collection.mutable.ListBuffer

/**
  * Created by C.J.YOU on 2016/2/24.
  */
object WeiBo extends  Platform {
  override val PLATFORM_NAME: String = "WeiBo"
  override val TOP_LEVEL_DOMAIN: String = "%weibo.com%"
  val urlListBuffer = new ListBuffer[String]()
  //主页
  def HomePage(url:String):Unit={
    val homepage = "http://user.qzone.qq.com/\\d{1,}/main".r
    val result  = homepage.findAllMatchIn(url)
    result.foreach(x => {
      urlListBuffer.+=(url)
    })
  }



}
