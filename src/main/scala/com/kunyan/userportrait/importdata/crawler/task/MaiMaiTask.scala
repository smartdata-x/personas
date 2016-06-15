package com.kunyan.userportrait.importdata.crawler.task

import java.util.concurrent.Callable

import com.kunyan.userportrait.importdata.crawler.request.MaiMaiRequest
import com.kunyan.userportrait.log.PLogger

import scala.collection.mutable

/**
  * Created by C.J.YOU on 2016/5/17.
  * 解析脉脉的多线程Task类
  */
class  MaiMaiTask(uidSet: mutable.HashSet[String], ua: String, cookie: String) extends Callable[mutable.HashSet[(String, mutable.HashMap[String, String])]] {

  override def call(): mutable.HashSet[(String, mutable.HashMap[String, String])] = {

    val set = new mutable.HashSet[(String, mutable.HashMap[String, String])]()
    PLogger.warn("Thread:" + Thread.currentThread().getName + ",activeCount:"+Thread.activeCount() + ",getId:"+Thread.currentThread().getId)
    uidSet.foreach( x => {

      val info = MaiMaiRequest.sendMaimaiRequest(x, ua, cookie)
      set.+=(info)

    })

    set

  }
}
