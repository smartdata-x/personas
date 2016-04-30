package com.kunyan.userportrait.importdata.extractor

import com.kunyan.userportrait.util.StringUtil

/**
  * Created by C.J.YOU on 2016/4/27.
  */
object Extractor extends Serializable{


  // qq,weibo,phone
  def extratorUserInfo(arr: Array[String]): (String,String,String) ={
    (phone(arr),qq(arr),weibo(arr))
  }
  // phone
  private def phone(array: Array[String]): String ={
    var info =""
    val cookie = array(6)
    if (cookie.contains("cSaveState=")) {
      val template = "(?<=cSaveState=)\\d{11}".r
      val resultPhone  =  template.findAllMatchIn(cookie)
      resultPhone.foreach(x => {
        info = x.toString
        return  info
      })
    } else if (cookie.contains("un=")) {
      val arr = cookie.split("un=")
      if (arr.length > 1) {
        val value = arr(1).split(";")(0)
        if (value forall Character.isDigit){
          info = value
          return  info
        }
      }
    }
    if(cookie.contains("idsLoginUserIdLastTime=")){
      val arr = cookie.split("idsLoginUserIdLastTime=")
      if (arr.length > 1) {
        val value = arr(1).split(";")(0)
        if (value forall Character.isDigit){
          info = value
          return  info
        }
      }
    }
    // dian ping
    if(cookie.contains("ua=")){
      val arr = cookie.split("ua=")
      if (arr.length > 1) {
        val value = arr(1).split(";")(0)
        if (value forall Character.isDigit ){
          if(value.toLong.toString.length == 11){
            info = value
            return  info
          }
        }
      }
    }
    // kfc
    if(cookie.contains("yum_ueserInfo=")){
      val arr = cookie.split("yum_ueserInfo=")
      if (arr.length > 1) {
        val value = arr(1).split(";")(0)
        if ( value forall Character.isDigit){
          if(value.toLong.toString.length == 11){
            info = value
            return  info
          }
        }
      }
    }
    info
  }
  // weibo id
  private def weibo(array: Array[String]): String ={
    var info =""
    val cookie = array(6)
    if (cookie.contains("SUS=SID-")) {
      val info = cookie.split("SUS=SID-")(1).split("-")(0)
      // println("info:"+info)
      return  info
    } else  if (cookie.contains("SUP=")) {
      try {
        val cookieValue = StringUtil.urlDecode (cookie)
        val name = cookieValue.indexOf ("uid=")
        if (name != -1) {
          val d = cookieValue.substring(name + 4).split ("&")(0)
          info = d.toString
          return  info
        }
      }catch {
        case e:Exception => println("weibo id url decode error")
      }
    } else  if(cookie.contains("wb_feed_unfolded_")) {
      val info = cookie.split ("wb_feed_unfolded_")(1).split ("=")(0)
      return  info
    }
    info
  }
  //  qq
  private def qq(array: Array[String]): String ={
    val cookie = array(6)
    var info =""
    if (cookie.contains("SUP=")) {
      try {
        val name = cookie.indexOf ("name=")
        if (name != -1) {
          val d = cookie.substring (name + 5).split (";")(0)
          val decode = StringUtil.urlDecode(d).split("&")(0)
          if (decode.contains ("%40qq.com")){
            val tempInfo = decode.replace ("%40qq.com", "")
            if(tempInfo forall Character.isDigit){
              return tempInfo
            }
          }
        }
      } catch {
        case e:Exception => println("qq url decode error")
      }
    } else if (cookie.contains("o_cookie=")) {
      val arr = cookie.split("o_cookie=")
      if (arr.length > 1) {
        if(arr(1).contains(";")){
          info = arr(1).split(";")(0)
          return  info
        }else{
          info = arr(1)
          return  info
        }
      }
    } else if (cookie.contains("qzone_check=")) {
      val arr = cookie.split("qzone_check=")
      if (arr.length > 1) {
        info = arr(1).split("_")(0)
        return  info
      }
    } else if(cookie.contains("pt2gguin=o")){
      val arr = cookie.split("pt2gguin=o")
      if (arr.length > 1) {
        if(arr(1).contains(";")){
          info = arr(1).split(";")(0)
        }else{
          info = arr(1)
        }
        if(info.nonEmpty){
          info = info.toLong.toString
          return  info
        }
      }
    }
    // dian ping
    if(cookie.contains("ua=")){
      val arr = cookie.split("ua=")
      if (arr.length > 1) {
        val value = arr(1).split(";")(0)
        if(value.contains("%40qq.com")){
          val tempInfo = value.replace("%40qq.com","")
          if (tempInfo forall Character.isDigit ){
            info = tempInfo.toLong.toString
            return  info
          }
        }

      }
    }
    // kfc
    if(cookie.contains("yum_ueserInfo=")) {
      val arr = cookie.split("yum_ueserInfo=")
      if (arr.length > 1) {
        val value = arr(1).split(";")(0)
        if(value.contains("%40qq.com")){
          val tempInfo = value.replace("%40qq.com","")
          if (tempInfo forall Character.isDigit ){
            info = tempInfo.toLong.toString
            return  info
          }
        }
      }
    }
    info
  }

  // email
  private  def email(array: Array[String]): String ={
    val cookie = array(6)
    var info =""
    if (cookie.contains("cSaveState=")) {
      val template = "(?<=cSaveState=)\\w+@\\w+\\.com".r
      val resultPhone  =  template.findAllMatchIn(cookie)
      resultPhone.foreach(x => {
        info = x.toString()
        return  info
      })
    } else if (cookie.contains("un=")) {
      val templateUser = "(?<=un=)\\w+@\\w+\\.com".r
      val userPhone  =  templateUser.findAllMatchIn(cookie)
      userPhone.foreach(x => {
        info = x.toString()
        return  info
      })
    }
    if(cookie.contains("idsLoginUserIdLastTime=")){
      val template = "(?<=idsLoginUserIdLastTime=)\\w+%40\\w+\\.com".r
      val resultPhone  =  template.findAllMatchIn(cookie)
      resultPhone.foreach(x => {
        info = x.toString().replace("%40","@")
        return  info
      })
    }
    info
  }
}
