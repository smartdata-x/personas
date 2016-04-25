import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by lcm on 2016/3/28.
 */
object RichManInfo {

  val setAllPhone = new scala.collection.mutable.TreeSet[String]()
  val listRichInfo = new scala.collection.mutable.ListBuffer[String]()

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("RICH MAN INFO")

    val sc = new SparkContext(sparkConf)

    //将用户的所有手机号码输出到文件
    val writerRichPhone = new PrintWriter(new File("F:\\资料\\hadoop\\rich_info\\rich_phone"), "UTF-8")
    //输出的股票用户rich_info
    val writerRichInfo = new PrintWriter(new File("F:\\资料\\hadoop\\rich_info\\rich_info"), "UTF-8")

    //将饿了么手机号数据读取到set集合
    sc.textFile("F:\\资料\\hadoop\\rich_info\\phone\\eleme_phone\\*").map(x => {
      val arr = x.split("\t")
      if (arr.length == 3) {
        (arr(0) + "\t" + arr(2))
      } else {
        null
      }
    }).filter(x => x != null).foreach(x => setAllPhone.+=(x))

    //将suning手机号数据读取到set集合
    sc.textFile("F:\\资料\\hadoop\\rich_info\\phone\\suning_phone\\*").map(x => {
      val arr = x.split("\t")
      if (arr.length > 3) {
        (arr(1) + "\t" + arr(3))
      } else {
        null
      }
    }).filter(x => x != null).foreach(x => setAllPhone.+=(x))

    //将weibo手机号数据读取到set集合
    sc.textFile("F:\\资料\\hadoop\\rich_info\\phone\\weibo_phone\\*").map(x => {
      val arr = x.split("\t")
      if (arr.length > 3) {
        (arr(1) + "\t" + arr(3))
      } else {
        null
      }
    }).filter(x => x != null).foreach(x => setAllPhone.+=(x))


    //将email读取到Set
    val email = sc.textFile("F:\\资料\\hadoop\\rich_info\\qq\\email\\*")
      .map(x => {
        val arr = x.split("\t")
        if (arr.length == 2) {
          (arr(0), arr(1))
        } else {
          null
        }
      }).filter(x => x != null).groupByKey()

    //将qq读取到set
    val qq = sc.textFile("F:\\资料\\hadoop\\rich_info\\qq\\qq\\*")
      .map(x => {
        val arr = x.split("\t")
        if (arr.length == 2) {
          (arr(0), arr(1))
        } else {
          null
        }
      }).filter(x => x != null).groupByKey()

    val ad = sc.textFile("F:\\资料\\hadoop\\rich_info\\ad\\merge_ad").map(x => {
      val arr = x.split("\t")
      (arr(0), arr(1))
    })

    for (x <- setAllPhone) {
      writerRichPhone.write(x + "\n")
    }
    writerRichPhone.close()

    val AllPhoneRdd = sc.textFile("F:\\资料\\hadoop\\rich_info\\rich_phone").map(x => {
      val arr = x.split("\t")
      if (arr.length == 2) {
        (arr(0), arr(1))
      } else {
        null
      }
    }).filter(x => x != null).groupByKey()
    //匹配出用户的手机号码
    val ad_phone = ad.leftOuterJoin(AllPhoneRdd)
      .map(x => {
        var str = ""
        if (x._2._2 == None) {
          x._1 + "\t" + x._2._1 + "\t" + "null"
        } else {
          x._2._2.foreach(i => {
            i.foreach(j => {
              str = str + "," + j
            })
          })
          x._1 + "\t" + x._2._1 + "\t" + str.replaceFirst(",", "")
        }
      }).map(x => {
      val arr = x.split("\t")
      (arr(0), arr(1) + "\t" + arr(2))
    })
    //匹配出手email
    val ad_phone_email = ad_phone.leftOuterJoin(email)
      .map(x => {
        var str = ""
        if (x._2._2 == None) {
          x._1 + "\t" + x._2._1 + "\t" + "null"
        } else {
          x._2._2.foreach(i => {
            i.foreach(j => {
              str = str + "," + j
            })
          })
          x._1 + "\t" + x._2._1 + "\t" + str.replaceFirst(",", "")
        }
      })
    //匹配QQ号
    ad_phone_email.map(x => {
      val arr = x.split("\t")
      (arr(0), arr(1) + "\t" + arr(2) + "\t" + arr(3))
    }).leftOuterJoin(qq)
      .map(x => {
        var str = ""
        val arr = x._2._1.split("\t")
        if (arr(2) == "null") {
          x._2._2.foreach(i => {
            i.foreach(j => {
              str = str + "," + j + "@qq.com"
            })
          })
          if (str == "") {
            x._1 + "\t" + arr(0) + "\t" + arr(1) + "\t" + "null"
          } else {
            x._1 + "\t" + arr(0) + "\t" + arr(1) + "\t" + str.replaceFirst(",", "")
          }
        } else {
          x._1 + "\t" + arr(0) + "\t" + arr(1) + "\t" + arr(2)
        }
      })
      .filter(x => {
        val arr = x.split("\t")
        //arr.length < 4
        !(arr(2) == "null" && arr(3) == "null")
      }).sortBy(x =>x.split("\t")(1).toInt,false)
      .foreach(x => listRichInfo.+=(x))

    for (x <- listRichInfo) {
      writerRichInfo.write(x + "\n")
    }
    writerRichInfo.close()
  }


  //格式化数据
  def formatData(data: String): String = {
    var str = ""
    val arr = data.split("\t")
    if (arr.length == 2) {
      data + "\t" + "null"
    } else {
      for (i <- 0 until arr.length - 2) {
        str = str + "," + arr(2 + i)
      }
      arr(0) + "\t" + arr(1) + "\t" + str.replaceFirst(",", "")
    }
  }
}
