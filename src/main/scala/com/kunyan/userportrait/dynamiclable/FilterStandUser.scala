/***************************************************************************************
* Copyright @ 2015 ShanghaiKunyan. All rights reserved
* @filename : /opt/spark-1.2.2-bin-hadoop2.4/work/spark/SparkKafka/src/main/scala/FilterStandUser.scala
* @author   : Sunsolo
* Email     : wukun@kunyan-inc.com
* Date      : 2016-07-25 17:02
*************************************************************************************************/

package com.kunyan.userportrait.dynamiclable

import MatchRule._

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object FilterStandUser {

  def main(args: Array[String]) {

    val xmlHandle = XmlHandle("./config.xml")
    val suc = StandUserContext(xmlHandle)

    var matchUserByWeek: RDD[(String, Int)] = null

    for( i <- 1 to suc.dataDays) {

      var matchUserByDay: RDD[(String, Int)] = null

      for( j <- 0 to suc.dataHours) {

        val originData = suc.generateRdd(i, j).filter( x => {

          val elem = x.split("\t")
          elem.size >= 8

        }).map( x => {

          val elem = x.split("\t")
          (elem(1), elem(3), elem(4))

        })

        val matchUserByHour = originData
          .filter(ruleUrlData(_, suc.standUrl))
          .map( x => (x._1, 1))
          .reduceByKey(_ + _)

        if(matchUserByDay == null) {
          matchUserByDay = matchUserByHour
        } else {

          matchUserByDay = matchUserByDay.groupWith(matchUserByHour).map( x => {
            (x._1, x._2._1.size + x._2._2.size)
          })

        }
      }

      if(matchUserByWeek == null) {
        matchUserByWeek = matchUserByDay.filter( x => x._2 > suc.dayThreshold)
      } else {

        matchUserByWeek = matchUserByWeek
          .groupWith(matchUserByDay.filter( x => x._2 >= suc.dayThreshold))
          .map( x => (x._1, x._2._1.size + x._2._2.size))

      }
    }

    val standUser = matchUserByWeek.filter( x => {
      x._2 >= suc.weekThreshold
    })
  }
}


