package com.kunyan.userportrait.importdata.crawler.task

import java.util
import java.util.concurrent.{Callable, Executors, Future}

import com.kunyan.userportrait.log.PLogger

import scala.collection.mutable

/**
  * Created by C.J.YOU on 2016/5/17.
  * 解析脉脉的多线程Task类
  */
class  MaiMaiSubTask(uidSet: mutable.HashSet[String], ua: String, cookie: String) extends Callable[mutable.HashSet[(String,mutable.HashMap[String,String])]] {

  override def call(): mutable.HashSet[(String, mutable.HashMap[String,String])] = {

    val set = new mutable.HashSet[(String, mutable.HashMap[String,String])]()
    val tasks = new util.ArrayList[Callable[(String,mutable.HashMap[String,String])]]()

    val subEs = Executors.newFixedThreadPool(6)

    PLogger.warn("MaiMaiSubTask Thread:" + Thread.currentThread().getName + ",activeCount:"+Thread.activeCount() + ",getId:"+Thread.currentThread().getId)

    for(uid <- uidSet) {

      val maiMaiRunable = new MaiMaiSubRunable(uid, ua, cookie)
      tasks.add(maiMaiRunable)

    }
    val result:util.List[Future[(String,mutable.HashMap[String,String])]] = subEs.invokeAll(tasks)

    for(index <- 0 until result.size){

      val res = result.get(index).get()
      set.add(res)

    }
    subEs.shutdown()
    set
  }
}
