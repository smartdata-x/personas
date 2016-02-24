package com.kunyan.userportrait.platform

import scala.collection.mutable.ListBuffer

/**
  * Created by C.J.YOU on 2016/2/24.
  */
object ZhiHu extends  Platform{
  override val PLATFORM_NAME: String = "ZhiHu"
  override val TOP_LEVEL_DOMAIN: String = "%.zhihu.com%"
  val urlListBuffer = new ListBuffer[String]()
  // 主页
  def extractHomePage(url:String): Unit ={
    val templat = "https://www.zhihu.com/people/"
    if(url.contains(templat)){
      urlListBuffer.+=(url)
    }
  }
  // 提问与回答
  def questionAndAnwer(url:String):Unit={
    val question = "http://www.zhihu.com/question/"
    val answer = "http://www.zhihu.com/question/\\d{1,}/answer/\\d{1,}".r
    val result  = answer.findAllMatchIn(url)
    result.foreach(x => {
      println (url)
    })
  }
}
