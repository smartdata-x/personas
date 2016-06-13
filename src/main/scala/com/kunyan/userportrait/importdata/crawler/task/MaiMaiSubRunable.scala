package com.kunyan.userportrait.importdata.crawler.task

import java.util.concurrent.Callable
import com.kunyan.userportrait.importdata.crawler.request.MaiMaiRequest
import com.kunyan.userportrait.log.PLogger

import scala.collection.mutable

/**
  * Created by C.J.YOU on 2016/5/30.
  * 请求数据单个任务
  */
class MaiMaiSubRunable (uid: String, ua: String, cookie: String) extends Callable[(String,mutable.HashMap[String,String])] {

  override def call(): (String, mutable.HashMap[String,String])= {

    PLogger.warn("MaiMaiSubRunable Thread:" + Thread.currentThread().getName + ",activeCount:"+Thread.activeCount() + ",getId:"+Thread.currentThread().getId)
    val info = MaiMaiRequest.sendMaimaiRequest(uid, ua, cookie)
    info

  }

}
