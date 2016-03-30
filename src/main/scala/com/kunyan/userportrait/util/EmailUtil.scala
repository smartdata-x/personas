package com.kunyan.userportrait.util

import com.kunyan.userportrait.rule.url.{SuNing, WeiBo}

import scala.collection.mutable

/**
  * Created by C.J.YOU on 2016/2/29.
  */
object EmailUtil {

  def email(lines:String): String={
    var email= "Nothing"
    val emailSet = new mutable.HashSet[String]()
    val tmpEmail = WeiBo.getEmail(lines)
    val suNingEmail = SuNing.getEmail(lines)
    if(!"".equals(tmpEmail)){
      emailSet.+=(tmpEmail)
    }
    if(!"".equals(suNingEmail)){
      emailSet.+=(suNingEmail)
    }
    email = emailSet.mkString(",").toString
    email
  }

}
