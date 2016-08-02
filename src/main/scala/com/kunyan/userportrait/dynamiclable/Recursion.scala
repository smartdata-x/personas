/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /home/wukun/work/SparkKafka/src/main/scala/Recursion.scala
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-05-17 12:41
#    Description  : 
=============================================================================*/
package com.kunyan.userportrait.dynamiclable

import com.kunyan.userportrait.dynamiclable.MatchRule.matchUrl

import org.apache.spark.rdd.RDD

import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer

object Recursion {

  type Tuple2List = Tuple2[(String, String), List[String]]
  type ArrayRDDList = ArrayBuffer[RDD[(String, List[String])]]
  type ArrayRDD = ArrayBuffer[RDD[(String, String)]]

  /**
    * @param ruleUrl 匹配的每天规则url
    * @param misMatch 没有匹配的url 
    */
  def misUrlDay (
    /* user_id和时间戳列表, 时间戳用来做选择扩充url的基准 */
    ruleUrl: RDD[(String, List[String])], 
    /* 每一维一天 */
    misMatch: ArrayBuffer[ArrayRDDList], 
    out: Int, 
    in: Int): ArrayRDD = {

    if(in == 0) {

      new ArrayRDD() += ruleUrl.join(misMatch(out)(in)).filter(matchUrl(_)).map( x => 
          /* 利用pagerank时src在前，dst在后，所以交换上级url和当前url的位置 */
          (x._2._2(1), x._2._2(0)))

    } else {

      misUrlDay(ruleUrl, misMatch, out, in - 1) += ruleUrl.join(misMatch(out)(in)).filter(matchUrl(_)).map( x => 
          (x._2._2(1), x._2._2(0)))

    }
  }

  def misUrlWeek(
    /* user_id和count */
    userRule: RDD[(String, Int)], 
    /* user_id和时间戳列表 */
    dayRules: ArrayRDDList, 
    mis: ArrayBuffer[ArrayRDDList], 
    out: Int, 
    in: Int): ArrayBuffer[ArrayRDD] = {

    if(out == 0) {

      val rule = userRule.join(dayRules(out)).map( x => {
        /* user_id和时间戳列表 */
        (x._1, x._2._2)
      })

      ArrayBuffer[ArrayRDD]() += misUrlDay(rule, mis, 0, in)

    } else {

      val rule = userRule.join(dayRules(out)).map( x => {
        (x._1, x._2._2)
      })

      misUrlWeek(userRule, dayRules, mis, out - 1, in) += misUrlDay(rule, mis, out, in)
    }
  }

  def extUrlDay(
    urlRef: ArrayBuffer[ArrayRDD], 
    out: Int, 
    in: Int): RDD[String] = {

    if(in == 0) {

      urlRef(out)(0).map( x => (x._1)).distinct

    } else {

      urlRef(out)(in).map( x => (x._1)).distinct ++ extUrlDay(urlRef, out, in - 1)

    }
  }

  def extUrlWeek(
    urlRef: ArrayBuffer[ArrayRDD], 
    out: Int, 
    in: Int): RDD[String] = {

    if(out == 0) {

      extUrlDay(urlRef, 0, in)

    } else {

      extUrlDay(urlRef, out, in) ++ extUrlWeek(urlRef, out - 1, in)

    }
  }
}
