package com.kunyan.userportrait.importdata.crawler.task

import java.util.concurrent.Callable
import com.kunyan.userportrait.importdata.crawler.request.MeiTuanRequest
import com.kunyan.userportrait.log.PLogger

import scala.collection.mutable

/**
  * Created by C.J.YOU on 2016/5/17.
  * 解析meituan的多线程Task类
  */
class  MeiTuanTask(ua: String, cookie: String) extends Callable[(String,mutable.HashMap[String,String])] {

  override def call(): (String, mutable.HashMap[String,String])= {

    val set = new mutable.HashSet[(String, mutable.HashMap[String,String])]()
    PLogger.warn("Thread:" + Thread.currentThread().getName + ",activeCount:"+Thread.activeCount() + ",getId:"+Thread.currentThread().getId)
    val info = MeiTuanRequest.sendMeiTuanRequest(ua, cookie)
    info

  }
}
