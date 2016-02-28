package com.kunyan.userportrait.util

import sun.misc.BASE64Decoder

/**
  * Created by yangshuai on 2016/2/27.
  */
object StringUtil {

  def decodeBase64(base64String : String): String = {
    val decoded = new BASE64Decoder().decodeBuffer(base64String)
    new String(decoded,"utf-8")
  }

}
