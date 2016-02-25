package com.kunyan.userportrait.platform

import scala.collection.mutable.ListBuffer

/**
  * Created by C.J.YOU on 2016/2/24.
  */
object Qzone extends  Platform {
  override val TOP_LEVEL_DOMAIN: String = "%.qq.com%"
  override val PLATFORM_NAME: String = "QQ"
  val urlListBuffer = new ListBuffer[String]()
  val QQListBuffer = new ListBuffer[String]()
  //个人中心
  def PersonCenter(url:String):Unit={
    val personcenterone = "http://user.qzone.qq.com/\\d{1,}/infocenter?ptsig=".r
    val personcentertwo = "http://user.qzone.qq.com/\\d{1,}/infocenter?via=toolbar".r
    val resultone  = personcenterone.findAllMatchIn(url)
    val resulttwo  = personcentertwo.findAllMatchIn(url)
    if(resultone != null) {
      resultone.foreach(x => {
        val QQone = url.split("/")(3)
        urlListBuffer.+=(url)
        QQListBuffer. +=(QQone)
      })
    }else{
      if( resulttwo != null) {
        resulttwo.foreach(x => {
          val QQtwo = url.split("/")(3)
          urlListBuffer.+=(url)
          QQListBuffer. +=(QQtwo)
        })
      }
    }
  }
  //主页
  def HomePage(url:String):Unit={
    val homepage = "http://user.qzone.qq.com/\\d{1,}/main".r
    val result  = homepage.findAllMatchIn(url)
    result.foreach(x => {
      val QQ = url.split("/")(3)
      urlListBuffer.+=(url)
      QQListBuffer. +=(QQ)
    })
  }
  //相册
  def PhotoAlbum(url:String):Unit={
    val photoalbum = "http://user.qzone.qq.com/\\d{1,}/4".r
    val result  = photoalbum.findAllMatchIn(url)
    result.foreach(x => {
      val QQ = url.split("/")(3)
      urlListBuffer.+=(url)
      QQListBuffer. +=(QQ)
    })
  }
  //留言板
  def MessageBoard(url:String):Unit={
    val messageboard = "http://user.qzone.qq.com/\\d{1,}/334".r
    val result  = messageboard.findAllMatchIn(url)
    result.foreach(x => {
      val QQ = url.split("/")(3)
      urlListBuffer.+=(url)
      QQListBuffer. +=(QQ)
    })
  }
  //时光轴
  def Time(url:String):Unit={
    val time = "http://user.qzone.qq.com/\\d{1,}/main?mode=gfp_timeline".r
    val result  = time.findAllMatchIn(url)
    result.foreach(x => {
      val QQ = url.split("/")(3)
      urlListBuffer.+=(url)
      QQListBuffer. +=(QQ)
    })
  }
  //更多
  def More(url:String):Unit={
    val more = "http://user.qzone.qq.com/\\d{1,}/more".r
    val result  = more.findAllMatchIn(url)
    result.foreach(x => {
      val QQ = url.split("/")(3)
      urlListBuffer.+=(url)
      QQListBuffer. +=(QQ)
    })
  }
  //个人档
  def PersonalFile(url:String):Unit={
    val personalfile = "http://user.qzone.qq.com/\\d{1,}/profile".r
    val result  =  personalfile.findAllMatchIn(url)
    result.foreach(x => {
      val QQ = url.split("/")(3)
      urlListBuffer.+=(url)
      QQListBuffer. +=(QQ)
    })
  }
  //好友
  def Friend(url:String):Unit={
    val friend = "http://user.qzone.qq.com/\\d{1,}/myhome/friends/center".r
    val result  =  friend.findAllMatchIn(url)
    result.foreach(x => {
      val QQ = url.split("/")(3)
      urlListBuffer.+=(url)
      QQListBuffer. +=(QQ)
    })
  }
  //应用
  def Application (url:String):Unit={
    val application  = "http://user.qzone.qq.com/\\d{1,}/appstore?via=qzoneframetop_nored".r
    val result  =  application .findAllMatchIn(url)
    result.foreach(x => {
      val QQ = url.split("/")(3)
      urlListBuffer.+=(url)
      QQListBuffer. +=(QQ)
    })
  }
  //装扮
  def Dress (url:String):Unit= {
    val dress = "http://user.qzone.qq.com/\\d{1,}/onekeydressup".r
    val result = dress.findAllMatchIn(url)
    result.foreach(x => {
      val QQ = url.split("/")(3)
      urlListBuffer.+=(url)
      QQListBuffer.+=(QQ)
    })
  }
    //空间设置
    def InterSpace (url:String):Unit={
      val inerspace  = "http://user.qzone.qq.com/\\d{1,}/profile/permit".r
      val result  =  inerspace .findAllMatchIn(url)
      result.foreach(x => {
        val QQ = url.split("/")(3)
        urlListBuffer.+=(url)
        QQListBuffer. +=(QQ)
      })
  }

}
