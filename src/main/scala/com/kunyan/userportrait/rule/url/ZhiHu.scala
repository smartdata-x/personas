package com.kunyan.userportrait.rule.url

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
    url.contains("http://www.zhihu.com/people") || (url == "http://www.zhihu.com/")
  }

  def getUrl(line:String): String ={
    val url = line.split("\t")(2)
    if(extractHomePage(url)) url else "NoDef"
  }

}
