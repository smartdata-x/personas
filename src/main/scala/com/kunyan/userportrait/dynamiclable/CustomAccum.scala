/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /home/wukun/work/SparkKafka/src/main/scala/CustomAccum.scala
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-05-27 10:36
#    Description  : 
=============================================================================*/
package com.kunyan.userportrait.dynamiclable

import org.apache.spark.Accumulator
import org.apache.spark.AccumulatorParam
import scala.collection.mutable.HashMap

/**
  * Created by wukun on 2016/7/11
  * 自定义累加器类
  */
object CustomAccum {

  type HashHashMap = HashMap[String, HashMap[String, Int]]

  implicit object HashMapAccumulatorParam extends AccumulatorParam[HashHashMap] {

   /**
     * 累加hashmap类型变量
     * @param t1 被累加变量
     * @param t2 累加变量
     * @return 累加后的值
     * @author wukun
     */
    def addInPlace(
      t1: HashHashMap, 
      t2: HashHashMap): HashHashMap = {

      val outerKeys = t2.keys

      for( outerKey <- outerKeys) {

        if(t1.contains(outerKey)) {

          val innerKeys = t2(outerKey).keys

          for(innerKey <- innerKeys) {

            if(t1(outerKey).contains(innerKey)) {
              t1(outerKey)(innerKey) = t1(outerKey)(innerKey) + t2(outerKey)(innerKey)
            }

          }

        }
      }

      t1
    }

    def zero(initialValue: HashHashMap): HashHashMap = initialValue
  }
}
