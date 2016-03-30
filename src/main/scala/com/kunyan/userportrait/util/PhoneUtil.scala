package com.kunyan.userportrait.util


import com.kunyan.userportrait.rule.url._

import scala.collection.mutable

/**
  * Created by C.J.YOU on 2016/2/26.
  */
object PhoneUtil {
  def getPhone(lines:String): String ={
    var phone = "Nothing"
    val phoneSet = new mutable.HashSet[String]()
    val suNingPhone  = SuNing.phone(lines)
    val eleMePhone = EleMe.phone(lines)
    val weiBoPhone = WeiBo.getPhone(lines)

    phoneSet.++=(suNingPhone)
    phoneSet.++=(weiBoPhone)

    if(!"".equals(eleMePhone))
      phoneSet.+=(eleMePhone)

    if(phoneSet.nonEmpty)
      phone = phoneSet.mkString(",").toString
    phone

  }

}
