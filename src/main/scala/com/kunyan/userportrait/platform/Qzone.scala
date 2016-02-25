package com.kunyan.userportrait.platform

import scala.collection.mutable.ListBuffer

/**
  * Created by C.J.YOU on 2016/2/24.
  */
object Qzone extends  Platform {
  override val TOP_LEVEL_DOMAIN: String = "%.qq.com%"
  override val PLATFORM_NAME_HTTP: String = "QQHttp"
  override val PLATFORM_NAME_INFO: String = "QQ"
  val urlListBuffer = new ListBuffer[String]()
  val QQListBuffer = new ListBuffer[String]()
  var QQ = new String
  //个人中心
  def PersonCenter(url:String):Boolean={
    var flag = false
    val personcenterone = "http://user.qzone.qq.com/\\d{1,}/infocenter?ptsig=".r
    val personcentertwo = "http://user.qzone.qq.com/\\d{1,}/infocenter?via=toolbar".r
    val resultone  = personcenterone.findAllMatchIn(url)
    val resulttwo  = personcentertwo.findAllMatchIn(url)
    if(resultone != null) {
      resultone.foreach(x => {
        val QQone = url.split("/")(3)
        flag =true
        QQ = QQone
      })
    }else{
      if( resulttwo != null) {
        resulttwo.foreach(x => {
          val QQtwo = url.split("/")(3)
          flag =true
          QQ = QQtwo
        })
      }
    }
    flag
  }
  //主页
  def HomePage(url:String):Boolean={
    var flag = false
    val homepage = "http://user.qzone.qq.com/\\d{1,}/main".r
    val result  = homepage.findAllMatchIn(url)
    result.foreach(x => {
      val QQzone = url.split("/")(3)
      flag =true
      QQ = QQzone
    })
    flag
  }
  //相册
  def PhotoAlbum(url:String):Boolean={
    var flag = false
    val photoalbum = "http://user.qzone.qq.com/\\d{1,}/4".r
    val result  = photoalbum.findAllMatchIn(url)
    result.foreach(x => {
      val QQzone = url.split("/")(3)
      flag =true
      QQ = QQzone
    })
    flag
  }
  //留言板
  def MessageBoard(url:String):Boolean={
    var flag = false
    val messageboard = "http://user.qzone.qq.com/\\d{1,}/334".r
    val result  = messageboard.findAllMatchIn(url)
    result.foreach(x => {
      val QQzone = url.split("/")(3)
      flag =true
      QQ = QQzone
    })
    flag
  }
  //时光轴
  def Time(url:String):Boolean={
    var flag = false
    val time = "http://user.qzone.qq.com/\\d{1,}/main?mode=gfp_timeline".r
    val result  = time.findAllMatchIn(url)
    result.foreach(x => {
      val QQzone = url.split("/")(3)
      flag =true
      QQ = QQzone
    })
    flag
  }
  //更多
  def More(url:String):Boolean={
    var flag = false
    val more = "http://user.qzone.qq.com/\\d{1,}/more".r
    val result  = more.findAllMatchIn(url)
    result.foreach(x => {
      val QQzone = url.split("/")(3)
      flag =true
      QQ = QQzone
    })
    flag
  }
  //个人档
  def PersonalFile(url:String):Boolean={
    var flag = false
    val personalfile = "http://user.qzone.qq.com/\\d{1,}/profile".r
    val result  =  personalfile.findAllMatchIn(url)
    result.foreach(x => {
      val QQzone = url.split("/")(3)
      flag =true
      QQ = QQzone
    })
    flag
  }
  //好友
  def Friend(url:String):Boolean={
    var flag = false
    val friend = "http://user.qzone.qq.com/\\d{1,}/myhome/friends/center".r
    val result  =  friend.findAllMatchIn(url)
    result.foreach(x => {
      val QQzone = url.split("/")(3)
      flag =true
      QQ = QQzone
    })
    flag
  }
  //应用
  def Application (url:String):Boolean={
    var flag = false
    val application  = "http://user.qzone.qq.com/\\d{1,}/appstore?via=qzoneframetop_nored".r
    val result  =  application .findAllMatchIn(url)
    result.foreach(x => {
      val QQzone = url.split("/")(3)
      flag =true
      QQ = QQzone
    })
    flag
  }
  //装扮
  def Dress (url:String):Boolean= {
    var flag = false
    val dress = "http://user.qzone.qq.com/\\d{1,}/onekeydressup".r
    val result = dress.findAllMatchIn(url)
    result.foreach(x => {
      val QQzone = url.split("/")(3)
      flag =true
      QQ = QQzone
    })
    flag
  }
    //空间设置
    def InterSpace (url:String):Boolean={
      var flag = false
      val inerspace  = "http://user.qzone.qq.com/\\d{1,}/profile/permit".r
      val result  =  inerspace .findAllMatchIn(url)
      result.foreach(x => {
        val QQzone = url.split("/")(3)
        flag =true
        QQ = QQzone
      })
      flag
  }

  def QQzone(line:String): Unit ={
    val  lineSplit = line.split("\t")
    val ad = lineSplit(0)
    val ua = lineSplit(1)
    val url = lineSplit(2)
    if(PersonCenter(url)){
      urlListBuffer.+=(line)
      QQListBuffer. += (ad +"\t"+ua+"\t"+QQ +"@qq.com")
    }else if(HomePage(url)){
      urlListBuffer.+=(line)
      QQListBuffer. += (ad +"\t"+ua+"\t"+QQ +"@qq.com")
    }else if(PhotoAlbum(url)){
      urlListBuffer.+=(line)
      QQListBuffer. += (ad +"\t"+ua+"\t"+QQ +"@qq.com")
  }else if(MessageBoard(url)){
      urlListBuffer.+=(line)
      QQListBuffer. += (ad +"\t"+ua+"\t"+QQ +"@qq.com")
    }else if(Time(url)){
      urlListBuffer.+=(line)
      QQListBuffer. += (ad +"\t"+ua+"\t"+QQ +"@qq.com")
    }else if(More(url)){
      urlListBuffer.+=(line)
      QQListBuffer. += (ad +"\t"+ua+"\t"+QQ +"@qq.com")
    }else if(PersonalFile(url)){
      urlListBuffer.+=(line)
      QQListBuffer. += (ad +"\t"+ua+"\t"+QQ +"@qq.com")
    }else if(Friend(url)){
      urlListBuffer.+=(line)
      QQListBuffer. += (ad +"\t"+ua+"\t"+QQ +"@qq.com")
    }else if(Application(url)){
      urlListBuffer.+=(line)
      QQListBuffer. += (ad +"\t"+ua+"\t"+QQ  +"@qq.com")
    }else if(Dress(url)){
      urlListBuffer.+=(line)
      QQListBuffer. += (ad +"\t"+ua+"\t"+QQ  +"@qq.com")
    }else if(InterSpace(url)){
      urlListBuffer.+=(line)
      QQListBuffer. += (ad +"\t"+ua+"\t"+QQ  +"@qq.com")
    }
  }
}
