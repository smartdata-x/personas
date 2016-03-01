package com.kunyan.userportrait.util

import com.kunyan.userportrait.rule.url._
/**
  * Created by C.J.YOU on 2016/2/26.
  */
object WeiBoUtil {

  def getWeiBoNumber(line:String): String ={
    var weiBoId= "Nothing"
    val tmpWeiBoId =  WeiBo.getWeiBoId(line)
    if(!"".equals(tmpWeiBoId)){
      weiBoId = tmpWeiBoId
    }
    weiBoId
  }
}
