package com.kunyan.userportrait.util

import com.kunyan.userportrait.rule.url.WeiBo

/**
  * Created by C.J.YOU on 2016/2/29.
  */
object EmailUtil {

  def email(lines:String): String={
    var email= "Nothing"
    val tmpEmail = WeiBo.getEmail(lines)
    if(!"".equals(tmpEmail)){
      email = tmpEmail
    }
    email
  }

}
