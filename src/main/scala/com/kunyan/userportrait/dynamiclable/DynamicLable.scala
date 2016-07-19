/*************************
 * Copyright @ 2015 ShanghaiKunyan. All rights reserved
 * @filename : /opt/spark-1.2.2-bin-hadoop2.4/work/spark/LabelScala/src/main/scala/com/kunyan/dynamiclabel/UserFilter.scala
 * @author   : Sunsolo
 * Email     : wukun@kunyan-inc.com
 * Date      : 2016-07-10 18:32
 **************************************/
package com.kunyan.userportrait.dynamiclable

import MatchRule._
import Recursion._

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.matching.Regex

object DynamicLable {

  def main(args: Array[String]) {

    val xmlHandle = XmlHandle("./config.xml")

    val days = xmlHandle.getElem("file", "days").toInt
    val hours = xmlHandle.getElem("file", "hours").toInt
    val dayThreshold = xmlHandle.getElem("file", "daythrd").toInt
    val weekThreshold = xmlHandle.getElem("file", "weekthrd").toInt
    val accu = xmlHandle.getElem("file", "accuracy").toDouble

    val pattern = """(http://|\b)(([^/]*)/|([^/]*)).*""".r

    val lines = Source.fromFile("/home/wukun/work/SparkKafka/src/main/scala/rule_url.txt").getLines().toArray
    val ruleUrl = lines(0).split(",")

    val conf = new SparkConf().setAppName("dynamicLable")
    val ctx = new SparkContext(conf)

    /** 
     * 每个数据文件中经过reduceByKey后用户访问次数
     **/
    var userTime: RDD[(String, String)] = null
    /**
     * 累积的一周userCountByDay
     **/
    var userCountByWeek: RDD[(String, Int)] = null

    val eachDayRules = new ArrayRDDList()


    for( i <- 0 to days) {
      /** 
       * 所有数据经过reduceByKey后总的用户访问次数
       **/
      var idToTimeListByDay: RDD[(String, List[String])] = null
      /**
       * 每一天的访问次数大于某一阈值的用户
       **/
      var userCountByDay: RDD[(String, Int)] = null

      // val misMatch = new ArrayRDDList()

      for( j <- 0 to hours) {

        val fileName = "/user/wukun/data/teleData_" + i + "/" + j

        // 查找hdfs的默认块大小命令为：hdfs dfs -stat "%o" /user/wukun/data/teleData_0/0
        // textFile读取文件形成的RDD的分区数为文件的hdfs块数,在57.12上的块大小为134217728B
        // 当数据不够2个块大小时，则默认值为2，当数据为空时，则默认值为1
        val originData = ctx.textFile(fileName).distinct.filter( x => {
          val elem = x.split("\t")
          elem.size >= 8
        }).map( x => {
          val elem = x.split("\t")
          /**
            * 0：时间戳
            * 1：用户ID
            * 3: 本次访问URL的主机
            * 4: 本次访问URL的主机后的部分
            * 5: 上级访问的URL
            */
          ((elem(0), elem(1)), List[String](elem(3), elem(4), elem(5)))
        })

        // 像什么 filter, map等转换操作不会改变分区数
        val matchRecord = originData.filter(ruleUrlData(_, ruleUrl)).map( x => {
          /**
            * 时间戳，用户ID，本次访问URL的主机，本次访问URL的主机后的部分
            **/
          ((x._1._1, x._1._2), (x._2(0), x._2(1)))
        })

        val mismatchRecord = originData.filter(misRuleUrlData(_, ruleUrl)).map( x => {

          val ref = (x._2)(2) match {
            case pattern(first, second, three, four) => {

              if(three != null) {
                three
              } else {
                four
              }

            }
          }

          /**
            * 用户ID，本次URL的HOST，上级URL，时间戳
            */
          (x._1._2, List[String](x._2(0), ref, x._1._1))
        })

        mismatchRecord.saveAsObjectFile("/user/wukun/misMatch/" + i + "/" + j)

        /* 根据键为标准, 组合不同的时间戳 */
        // (用户ID， 时间戳列表)
        val idToTimeList = matchRecord.map(x => (x._1._2, x._1._1)).combineByKey(
          (v : String) => List(v),
          (c : List[String], v : String) => v :: c,
          (c1: List[String], c2: List[String]) => c1:::c2
        ).coalesce(matchRecord.partitions.length / 5 + 1)

        /* 用户每天的数据：格式为用户ID：时间戳列表 */
        if(idToTimeListByDay == null) {

          idToTimeListByDay = idToTimeList

        } else {

          // 在到达unio之前的分区数都没有改变，当然union也没有改变单个RDD的分区数，它只是将所有的分区数整合到一起
          // 因为单个task是和单个partition对应的，所以partiton越多task也就越多，当然为了达到一定的平行度这样做是可以的。
          // 但当单个task任务数据量太少时，就会产生过多的任务，导致提交次数太多
          idToTimeListByDay = idToTimeListByDay.union(idToTimeList).combineByKey(
            (v : List[String]) => v,
            (c : List[String], v : List[String]) => v ::: c,
            (c1: List[String], c2: List[String]) => c1::: c2
          )

        }
      }

      // 在上面idToTimeListByDay将一天中24小时内的RDD全整合到一起，分区数是逐渐累加的

      // 到这个位置有misMatchs, userCount缓存(可以看得出容量比较大)  ********

      /**
        * 用户首先要满足每天的访问次数大于某一阈值 
        */
      idToTimeListByDay = idToTimeListByDay.coalesce(idToTimeListByDay.partitions.length / 8 + 1)

      val tmp = idToTimeListByDay.filter(x => x._2.size > dayThreshold).cache
      tmp.saveAsObjectFile("/user/wukun/eachDayRule/" + i)

      /** 
        * 此方法是让某人在一天内出现的次数回归为1 
        */
      userCountByDay = tmp.map( x => (x._1, 1))

      if(userCountByWeek == null) {
        userCountByWeek = userCountByDay
      } else {
        // userCountByWeek是将所有天数下面的小时里的所有RDD的分区进行了累加
        userCountByWeek = userCountByWeek.union(userCountByDay).reduceByKey( _ + _)
      }

    }
    // 到这个位置有misMatchs, eachDayRules, userCountByWeek会被缓存(可以看得出容量比较大)  ********

    /**
      * 根据一周出现的次数，筛选合适的user 
      */
    // userCountByWeek(用户ID, 出现次数)
    // standUserByWeek同上
    // standUserByWeek同userCountByWeek的分区数是一样的

    userCountByWeek = userCountByWeek.coalesce(userCountByWeek.partitions.length / 3 + 1)
    var standUserByWeek = userCountByWeek.filter(x => x._2 >= weekThreshold)

    userCountByWeek = null
    // 到这个位置有misMatchs, eachDayRules, standardUser会被缓存(可以看得出容量比较大)    *******

    /**
      * standardUser: 筛选出的规则用户
      * eachDayRules: 应该是规则用户对应的访问规则url的时间戳(用来限制匹配扩展url的范围)
      * mismatchRecord: 存储待扩展的url
      * misUrlAndRefs: (上级url，当前url)
      */
    for( i <- 0 to days) {

      // 当saveAsObjectFile保存以后，再从文件里读取后，分区数是不变的
      val standUserByDay = ctx.objectFile[(String, List[String])]("/user/wukun/eachDayRule/" + i)

      for( j <- 0 to hours) {

        val mismatchRecord = ctx.objectFile[(String, List[String])]("/user/wukun/misMatch/" + i + "/" + j)
        // join后的分区数是和调用join这个接口的RDD分区数一样的
        val standUser = standUserByWeek.join(standUserByDay).map( x => {
          (x._1, x._2._2)
        })

        // mismatchRecord用户ID，本次URL的HOST，上级URL，时间戳
        standUser.join(mismatchRecord).filter(matchUrl(_)).map( x => {
          /* 源url, 目的url */
          (x._2._2(1), x._2._2(0))
        }).saveAsObjectFile("/user/wukun/misHourUrl/" + i + "/" + j)

      }

    }

    standUserByWeek = null

    /* url(准备进行pagerank) */
    var extendUrl: RDD[String] = null

    for( i <- 0 to days ) {

      for( j <- 0 to hours ) {

        // 在下面misHourUrl指定的文件里，一个文件是99个分区, 99 * 48 = 4752, 这样分区多，产生大量的临时对象，造成垃圾回收困难
        val extendUrlByHour = ctx.objectFile[(String, String)]("/user/wukun/misHourUrl/" + i + "/" + j).map( x => x._2).distinct

        if(extendUrl == null) {
          extendUrl = extendUrlByHour
        } else {
          extendUrl = (extendUrl ++ extendUrlByHour).distinct
        }

      }
      extendUrl = extendUrl.coalesce(extendUrl.partitions.length / 7 + 1)
    }

    // 到这个位置有misUrlAndRefs, extendUrl被缓存(可以看得出容量比较大)    *********

    var id: Int = 0

    /**
      * 给扩展的URL添加标识，以数字为标识
      */
    val urlToCount = HashMap[String, Int]()
    val allExtendUrl = extendUrl.distinct.collect

    allExtendUrl.foreach( x => {
      urlToCount += (x -> id)
      id = id + 1
    })

    var relateUrl:Option[RDD[(Int, Int)]] = None

    for(i <- 0 to days) {

      for(j <- 0 to hours) {
        val matchUserMisUrl = ctx.objectFile[(String, String)]("/user/wukun/misHourUrl/" + i + "/" + j).filter( x => {

          if(urlToCount.contains(x._1) && urlToCount.contains(x._2)) {
            true
          } else {
            false
          }
        }).map( x => (urlToCount(x._1), urlToCount(x._2)))  // 最后是以标识来代表相关的url

        if(relateUrl.isDefined) {
          relateUrl = Some(relateUrl.get.union(matchUserMisUrl))
        } else {
          relateUrl = Some(matchUserMisUrl)
        }
      }
    }

    val origin = relateUrl match {
      case Some(relate) => relate.map( x => {
        new Edge[Int](x._1, x._2)
      })
      case None => System.exit(1)
    } 
    
    val graph = Graph.fromEdges[Int, Int](origin.asInstanceOf[RDD[Edge[Int]]], 0)
    //val graph = Graph.fromEdges[Int, Int](origin, 0)

    val ranks = graph.pageRank(accu).vertices
  }
}

