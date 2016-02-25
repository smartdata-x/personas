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
    val homepage = "http://weibo.com/\\d{1,}/profile?".r
    val result  = homepage.findAllMatchIn(url)
    result.foreach(x => {
      urlListBuffer.+=(url)
    })
  }
  //相册
  def PhotoAlbum(url:String):Unit={
    val photoalbum = "http://weibo.com/p/\\d{1,}/album?".r
    val result  = photoalbum.findAllMatchIn(url)
    result.foreach(x => {
      urlListBuffer.+=(url)
    })
  }
  //管理
  def Manage(url:String):Unit={
    val manage = "http://weibo.com/p/\\d{1,}/manage?".r
    val result  = manage.findAllMatchIn(url)
    result.foreach(x => {
      urlListBuffer.+=(url)
    })
  }
  //首页
  def Home(url:String):Unit={
    val home = "http://weibo.com/p/\\d{1,}/manage?".r
    val result  =home.findAllMatchIn(url)
    result.foreach(x => {
      urlListBuffer.+=(url)
    })
  }
  //发现
  def Found(url:String):Unit={
    val found = "http://d.weibo.com/\\d{6}".r
    val result  =found.findAllMatchIn(url)
    result.foreach(x => {
      urlListBuffer.+=(url)
    })
  }
  //个人信息
  def PersonInformation(url:String):Unit={
    val personinformation = "http://weibo.com/p/\\d{6}/nfo?mod=pedit".r
    val result  =personinformation.findAllMatchIn(url)
    result.foreach(x => {
      urlListBuffer.+=(url)
    })
  }



}
