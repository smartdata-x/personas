package com.kunyan.userportrait.platform

/**
  * Created by C.J.YOU on 2016/2/24.
  */
object PlatformScheduler {

  def apply(pType:Int): Platform ={
    if(pType == 1){
      Eleme
    }else if(pType == 2){
      Qzone
    }else if(pType == 3){
      ZhiHu
    }else if(pType == 4){
      WeiBo
    }else{
      SuNing
    }
  }
}
