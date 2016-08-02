/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /home/wukun/work/SparkKafka/src/main/scala/CustomOrder.scala
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-07-24 17:00
#    Description  : 
=============================================================================*/
package com.kunyan.userportrait.dynamiclable

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.TreeSet

object CustomOrder extends Ordering[(Long, Double)] {

  def compare(x: (Long, Double), y: (Long, Double)): Int = {
    if(x._2 > y._2) {
      1
    } else if (x._2 == y._2) {
      0
    } else {
      -1
    }
  }

  def main(args: Array[String]) {
    val set = new TreeSet[(Long, Double)]()(CustomOrder)
    set += ((3L, 0.54))
    set += ((2L, 0.34))
    set += ((1L, 0.65))

    set.foreach(println(_))
  }

}

