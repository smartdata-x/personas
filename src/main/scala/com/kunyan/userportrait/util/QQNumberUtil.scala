package com.kunyan.userportrait.util

import com.kunyan.userportrait.rule.url.Qzone

/**
  * Created by C.J.YOU on 2016/2/26.
  */
object QQNumberUtil {

  def QQNumber(lines:String): String ={
    var QQ= "Nothing"
    val tmpQQ = Qzone.getQQ(lines)
    if(!"".equals(tmpQQ)){
      QQ = tmpQQ
    }
    QQ
  }

}
