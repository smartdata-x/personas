import java.io.{File, PrintWriter}

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lcm on 2016/3/28.
 */
object RichManInfo {

  val setElemePhone = new scala.collection.mutable.TreeSet[String]()
  val setSuNingPhone = new scala.collection.mutable.TreeSet[String]()
  val setWeiBoPhone = new scala.collection.mutable.TreeSet[String]()
  val setEmail = new scala.collection.mutable.TreeSet[String]()
  val setQq = new scala.collection.mutable.TreeSet[String]()
  val setRichInfo = new scala.collection.mutable.TreeSet[String]()

  def main(args: Array[String]) {
    val file_in = args(0)
    val file_out = args(1)
    //val sparkConf = new SparkConf().setAppName("RICH MAN INFO").setMaster("local")
    val sparkConf = new SparkConf().setAppName("RICH MAN INFO")
    val sc = new SparkContext(sparkConf)

    //输出的股票用户rich_info
    //val writerRichInfo = new PrintWriter(new File("F:\\资料\\hadoop\\rich_info\\rich_info"), "UTF-8")
    val writerRichInfo = new PrintWriter(new File(file_out), "UTF-8")

    //将饿了么手机号数据读取到set集合
    val eleme_phone = sc.textFile("file://" + file_in + "rich_phone/eleme_phone/*").map(x => {
      //val eleme_phone = sc.textFile("F:\\资料\\hadoop\\rich_info\\phone\\eleme_phone\\*").map(x => {
      val arr = x.split("\t")
      if (arr.length == 3) {
        arr(0) + "\t" + arr(2)
      } else {
        null
      }
    }).filter(x => x != null)
      .foreach(x => {
        setElemePhone.+=(x)
      })
    //将suning手机号数据读取到set集合
    val suning_phone = sc.textFile("file://" + file_in + "rich_phone/suning_phone/*").map(x => {
      //val suning_phone = sc.textFile("F:\\资料\\hadoop\\rich_info\\phone\\suning_phone\\*").map(x => {
      val arr = x.split("\t")
      if (arr.length > 3) {
        arr(1) + "\t" + arr(3)
      } else {
        null
      }
    }).filter(x => x != null)
      .foreach(x => {
        setSuNingPhone.+=(x)
      })
    //将weibo手机号数据读取到set集合
    val weibo_phone = sc.textFile("file://" + file_in + "rich_phone/weibo_phone/*").map(x => {
      //val weibo_phone = sc.textFile("F:\\资料\\hadoop\\rich_info\\phone\\weibo_phone\\*").map(x => {
      val arr = x.split("\t")
      if (arr.length > 3) {
        arr(1) + "\t" + arr(3)
      } else {
        null
      }
    }).filter(x => x != null)
      .foreach(x => {
        setWeiBoPhone.+=(x)
      })

    //将email读取到Set
    val email = sc.textFile("file://" + file_in + "rich_email/*")
    //val email = sc.textFile("F:\\资料\\hadoop\\rich_info\\qq\\email\\*")
      .map(x => {
        val arr = x.split("\t")
        if (arr.length == 2) {
          arr(0) + "\t" + arr(1)
        } else {
          null
        }
      }).filter(x => x != null)
      .foreach(x => {
        setEmail.+=(x)
      })

    //将qq读取到set
    val qq = sc.textFile("file://" + file_in + "rich_qq/*")
    //val qq = sc.textFile("F:\\资料\\hadoop\\rich_info\\qq\\qq\\*")
      .map(x => {
        val arr = x.split("\t")
        if (arr.length == 2) {
          arr(0) + "\t" + arr(1)
        } else {
          null
        }
      }).filter(x => x != null)
      .foreach(x => {
        setQq.+=(x)
      })
    sc.textFile("file://" + file_in + "rich_ad/merge_ad")
    //sc.textFile("F:\\资料\\hadoop\\rich_info\\ad\\merge_ad")
          .map(x => {
            var str = x
            for (e <- setElemePhone) {
              val arr = e.split("\t")
              if (x == arr(0)) {
                str = str + "," + arr(1)
              }
            }
            for (s <- setSuNingPhone) {
              val arr = s.split("\t")
              if (x == arr(0)) {
                str = str + "," + arr(1)
              }
            }
            for (w <- setWeiBoPhone) {
              val arr = w.split("\t")
              if (x == arr(0)) {
                str = str + "," + arr(1)
              }
            }
            str
          }).map(x => {
      if (x.contains(",")) {
        x.replaceFirst(",", "\t")
      } else {
        //println(x + "\t" + "null")
        x + "\t" + "null"
      }
    }).map(x => {
      var str = x
      for (e <- setEmail) {
        val arr = e.split("\t")
        if (x.split("\t")(0) == arr(0)) {
          str = str + "\t" + arr(1)
        }
      }
      str
    }).map(x => {
      var str = x
      val arrData = x.split("\t")
      if (arrData.length == 2) {
        for (q <- setQq) {
          val arrQQ = q.split("\t")
          if (arrData(0) == arrQQ(0)) {
            str = str + "\t" + arrQQ(1) + "@qq.com"
            //println("str  =  " + str)
          }
        }
      }
      str
    }).filter(x => {
      val arr = x.split("\t")
      !(arr.length == 2 && arr(1) == "null")
    }).map(formatData)
      .foreach(x => {
        setRichInfo.+=(x)
        println(x)
      })

    for (x <- setRichInfo) {
      writerRichInfo.write(x)
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
