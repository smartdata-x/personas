package com.kunyan.userportrait.platform

import scala.collection.mutable.ListBuffer

/**
  * Created by C.J.YOU on 2016/2/24.
  */
object ZhiHu extends  Platform{
  override val PLATFORM_NAME_INFO: String = "ZhiHu"
  override val TOP_LEVEL_DOMAIN: String = "%.zhihu.com%"
  override val PLATFORM_NAME_HTTP: String = "ZhiHuHttp"

  val urlListBuffer = new ListBuffer[String]()

  // 主页
  def extractHomePage(url:String): Boolean ={
    var flag = false
    val templet = "http://www.zhihu.com/people"
    if(url.contains(templet)){
      flag = true
    }
    val templetDomain = "http://www.zhihu.com/"
    if(url == templetDomain){
      flag = true
    }
    flag
  }
  // 话题
  def extractTopic(url:String): Boolean ={
    var flag = false
    val templet = "http://www.zhihu.com/topic"
    if(url == templet){
      urlListBuffer.+=(url)
      flag = true
    }
    val templetTwo = "http://www.zhihu.com/topic/\\d{1,}".r
    val result  = templetTwo.findAllMatchIn(url)
    result.foreach(x => {
      flag = true
      // println(url)
    })
    flag
  }
  // 提问与回答
  def extractQuestionAndAnwer(url:String):Boolean={
    var flag = false
    val question = "http://www.zhihu.com/question/".r
    val answer = "http://www.zhihu.com/question/\\d{1,}/answer/\\d{1,}".r
    val result  = answer.findAllMatchIn(url)
    result.foreach(x => {
      flag = true
      // println(url)
    })
    val questionResult  = question.findAllMatchIn(url)
    questionResult.foreach(x => {
      flag  = true
      // println(url)
    })
    flag
  }

  def extract(line:String): Unit ={
    val url = line.split("\t")(2)
    if(extractHomePage(url)){
      urlListBuffer.+=(line)
    }else if(extractTopic(url)){
      urlListBuffer.+=(line)
    }else if(extractQuestionAndAnwer(url)){
      urlListBuffer.+=(line)
    }
  }

}
