/*************************
 * Copyright @ 2015 ShanghaiKunyan. All rights reserved
 * @filename : /opt/spark-1.2.2-bin-hadoop2.4/work/spark/LabelScala/src/main/scala/com/kunyan/dynamiclabel/MatchRule.scala
 * @author   : Sunsolo
 * Email     : wukun@kunyan-inc.com
 * Date      : 2016-07-11 18:32
 **************************************/
package com.kunyan.wokongsvc.userportrait

import Recursion._

/**
  * Created by wukun on 2016/7/11
  * URL匹配类
  */
object MatchRule {

  /**
    * 判断一个记录是否匹配标准url
    * @param elem 筛选的一条记录
    * @param url  标准的url
    * @return 匹配url的记录
    * @author wukun
    */
  def ruleUrlData(
    elem: Tuple2List, 
    url: Array[String]): Boolean = {

    if((elem._1)._2.contains(".") == true) {
      false
    } else {

      if((elem._2)(0).compareTo("www.news.cn") == 0) {
        (elem._2)(1).startsWith("/fortune")
      } else if((elem._2)(0).compareTo("moer.jiemian.com") == 0) {
        (elem._2)(1).startsWith("/investment_findPageList.htm?onColumns=TZGD_HUSHEN&industrys=all&fieldColumn=all&price=free&authorType=1&sortType=time")
      } else {

        url.exists(y => {
          if((elem._2)(0).compareTo(y) == 0) true else false
        })

      }
    }

  }

  /**
    * 判断一个记录匹配标准url
    * @param elem 筛选的一条记录
    * @param url  标准的url
    * @return 不匹配url的记录
    * @author wukun
    */
  def misRuleUrlData(
    elem: Tuple2List, 
    url: Array[String]): Boolean = {

    if((elem._2)(2).compareTo("NoDef") != 0) {

      if((elem._2)(2).startsWith("http") || (elem._2)(2).startsWith("www")) {

        if((elem._1)._2.contains(".") == true) {
          false
        } else {

          if((elem._2)(0).compareTo("www.news.cn") == 0) {
            !((elem._2)(1).startsWith("/fortune"))
          } else if((elem._2)(0).compareTo("moer.jiemian.com") == 0) {
            !((elem._2)(1).startsWith("/investment_findPageList.htm?onColumns=TZGD_HUSHEN&industrys=all&fieldColumn=all&price=free&authorType=1&sortType=time"))
          } else {
            !(url.exists(y => if((elem._2)(0).compareTo(y) == 0) true else false))
          }

        }

      } else {
        false
      }

    } else {
      false
    }
  }

  /* 
   * 用户ID、时间戳列表、list[URL的HOST, 上级URL, 时间戳] 
   * 时间戳列表: 用来筛选的时间基准
   * URL的HOST：相关时间内访问的其他URL
   * 时间戳: 访问筛选URL时的时间戳
   */
  def matchUrl(
    elem: Tuple2[String, Tuple2[List[String], List[String]]]
    ): Boolean = {

      elem._2._1.exists( y => {
        ((y.toDouble - 1000 * 60 * 1) <= (elem._2._2)(2).toDouble) && ((elem._2._2)(2).toDouble <= (y.toDouble + 1000 * 60 *5))
      })

  }
}
