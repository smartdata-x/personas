package com.kunyan.userportrait.util

import com.kunyan.userportrait.rule.url.{ZhiHu, SuNing, WeiBo, EleMe}

/**
  * Created by C.J.YOU on 2016/2/28.
  */
object UrlUtil {

  def getUrl(line:String):String ={
    val url = line.split("\t")(2)
    val eleMeUrl = EleMe.getUrl(line)
    //println(!eleMeUrl.equals("NoDef"))
    val weiBoUrl = WeiBo.getWeiBoUrl(line)
    // println(!weiBoUrl.equals("NoDef"))
    val suNingUrl = SuNing.getUrl(line)
    // println(!suNingUrl.equals("NoDef"))
    val zhiHuUrl = ZhiHu.getUrl(line)
    // println(!zhiHuUrl.equals("NoDef"))
    if(!eleMeUrl.equals("NoDef") || !weiBoUrl.equals("NoDef") || !suNingUrl.equals("NoDef") || !zhiHuUrl.equals("NoDef"))  url else "NoDef"
  }

}
