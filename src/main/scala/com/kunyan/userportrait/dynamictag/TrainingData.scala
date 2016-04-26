
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by wangcao on 2016/4/1.
  *
  * Create tranning dataset
  * target：分类：股票人-1，非股票人-0；
  *              有钱人-1，非有钱人-0。
  *
  */
object TrainingData {

  def getUrlRemoveHead (url: String): String = {
    var part = url
    try {
      if (url.contains("http://")) {
        part = url.replace("http://", "").split("/").filter(x => x.length > 0)(0)
      } else if (url.contains("https://")) {
        part = url.replace("https://", "").split("/").filter(x => x.length > 0)(0)
      } else {
        part = url.split("/").filter(x => x.length > 0)(0)
      }
    } catch {
      case e: Exception =>
    }
    part
  }

  def getUrlBody (url: String): String = {
    val host = getUrlRemoveHead(url)
    var part = host
    try {
      if (host.startsWith("www.")) {
        part = host.replace("www.", "")
      } else if (!host.contains("www")) {
        part = host
      }
    } catch {
      case e: Exception =>
    }
    part
  }

  def getUrlKey (url: String): String = {
    var part = url
    if (url.contains("http://www")) {
      part = url.substring(11, url.length)
    } else if (url.contains("https://www")) {
      part = url.substring(12, url.length)
    } else {
      part = url
    }
    part
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("TrainingData").setMaster("local")
    val sc = new SparkContext(conf)

    /**
      * 1. input and pre-process data
      */
    //1.1 input total data from telecom
    val data = sc.textFile(args(0)).map(_.split("\t"))
      .filter(x => x.size == 6).map(x => (x(1), x(2), getUrlBody(x(3)), getUrlBody(x(4))))
      .filter(x => !x._2.contains("spider") && !x._2.contains("Spider"))
      .filter(x => x._3 != "qq.com" && !x._3.contains("mail") && x._3 !="ip.com" && x._3 != "baidu.com" && x._3 != "so.com" && x._3 != "ti.com.cn" && x._3 != "le.com" && x._3 != "sina.com.cn" )
    data.cache()

    //1.2 input target url (only original url)
    val totalUrl = sc.textFile(args(1)).map(x => x.trim)
      .map(x => getUrlKey(x)).distinct().filter(x => x.length > 0).collect.mkString(",", ",", ",")

    /**
      * 2. calculate features for each ad and ip
      */
    val labelLineRddUr = data.filter(x => totalUrl.contains(x._3)).map(x => {
      val url = x._3
      val domain = url
      (x._1 + "\t" + domain, 1)
    }).reduceByKey(_ + _)
    val urlNumUr = labelLineRddUr.map(_._1.split("\t")).map(x => (x(0), 1)).reduceByKey(_ + _)

    val labelLineRddRe = data.filter(x => totalUrl.contains(x._4)).map(x => {
      val url = x._4
      val domain = url
       (x._1 + "\t" + domain, 1)
     }).reduceByKey(_ + _)
    val urlNumRe = labelLineRddRe.map(_._1.split("\t")).map(x => (x(0), 1)).reduceByKey(_ + _)

    /**
      * 3. label the positive and negative observations
      *
      * for stockman, if the number of target url or reference  is not less than 10 -->label 1
      *               if the numbers of target url and reference  are all equals only 1 -->label 0
      * for richman, if the number of target url or reference  is not less than 5 -->label 1
      *              if the numbers of target url and reference  are all equals only 1 -->label 0
      *
      * pay attention: the numbers of positive and negative observations need be balanced or reaching a optimal position.
      */
    val finalTable = urlNumUr.join(urlNumRe)
    val stockP = finalTable.filter(x => x._1.length == 40).filter(x => x._2._1 >= 10 || x._2._2 >= 10).map(x => (x._1, 1))
    val stockN = finalTable.filter(x => x._1.length == 40).filter(x => x._2._1 == 1 && x._2._2 == 1).map(x => (x._1, 0))
    val stockSample = stockP.union(stockN)

    /**
      * 4. match the four targeted features with the training observations according to the ad or ip
      *
      * pay attention: if the scales of features have large differences, it may be necessary to scale-down all of them
      */
    val features = sc.textFile(arg(2))
      .map(x => x.replace("(", "")).map(x => x.replace(")", ""))
      .map(_.split(","))
      .filter(x => x(0).length == 40)
      .filter(x => x.size == 5)
      .map(x => (x(0), (x(1).toDouble, x(2).toDouble, x(3).toDouble, x(4).toDouble)))

    val stockTrain1 = stockSample.join(features).values
    val stockTrain = stockTrain1.map(x =>
      try {
        x._1 + "," + x._2._1/1000 + " " + x._2._2/10 + " " + x._2._3/1000 + " " + x._2._4/10
      } catch {
        case e: Exception => null
      })

    stockTrain.filter(x=> x!= null).coalesce(1, true).saveAsTextFile(args(3))

    data.unpersist()
    sc.stop()
  }
}