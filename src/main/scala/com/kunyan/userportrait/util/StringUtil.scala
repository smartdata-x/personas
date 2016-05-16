package com.kunyan.userportrait.util

import java.io.ByteArrayOutputStream
import java.util.zip.InflaterOutputStream

import com.kunyan.userportrait.log.PLogger
import jodd.util.URLDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.json.JSONObject
import sun.misc.BASE64Decoder

/**
  * Created by yangshuai on 2016/2/27.
  */
object StringUtil {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("WEIBO")
    val sc = new SparkContext(conf)
    val input = sc.textFile("/user/portrait/result/weibodata/mid").map(_.split("\t")).filter(_.length==4)
    input.map(x=>(getUserId(x(3)), x)).filter(_._1.nonEmpty).groupByKey().map(onlyOne).filter(_.nonEmpty).saveAsTextFile("/user/portrait/result/weibodata/step1")

  }

  def decodeBase64(base64String : String): String = {
    val decoded = new BASE64Decoder().decodeBuffer(base64String)
    new String(decoded,"utf-8")
  }

  def getUserId(str: String): String = {

    val arr = decodeBase64(str).split("SUS=SID-")

    if (arr.length < 2)
      return ""

    val idArr = arr(1).split("-")

    if (idArr.length >= 2)
      return idArr(0)

    ""
  }

  def getResult(pair: (String, Array[String])): String = {

    val arr = pair._2

    if (arr == null)
      return ""

    val url = s"http://weibo.com/${pair._1}/profile"
    String.format("{\"ad\":\"%s\", \"ua\":\"%s\", \"url\":\"%s\", \"cookie\":\"%s\", \"platform\":\"weibo_step1\"}", arr(0), arr(1), url, arr(3))

  }

  def onlyOne(pair: (String, Iterable[Array[String]])): String = {

    val userId = pair._1
    val iterator = pair._2

    var result: Array[String] = null
    var timeStr = ""
    var timeStamp = 0l

    iterator.seq.foreach(arr => {
      val cookie = arr(3)
      val cookieArr = decodeBase64(cookie).split("SUS=SID-")
      if (cookieArr.length >= 2) {
        val timeArr = cookieArr(1).split("-")
        if (timeArr.length >= 2) {
          timeStr = timeArr(1)
          if (timeStr forall Character.isDigit) {
            if (timeStamp < timeStr.toLong) {
              timeStamp = timeStr.toLong
              result = arr
            }
          }
        }
      }
    })

    getResult((userId, result))
  }

  /**
    * @param string 源字符串
    * @return url解码后的字符串
    * @author youchaojiang
    */
  def urlDecode(string: String): String = {
    val decoder  = URLDecoder.decode(string,"utf-8")
    decoder
  }

  /**
    * @param compressedStr 压缩的字符串
    * @return 压缩后的字符串
    * @author youchaojiang
    */
  private def  zlibUnzip(compressedStr: String ): String = {

    if(compressedStr == null) {
      return null
    }
    val bos = new ByteArrayOutputStream()
    val  zos = new InflaterOutputStream(bos)
    try {
      zos.write(new sun.misc.BASE64Decoder().decodeBuffer(compressedStr))
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {

      if(zos != null ) {
        zos.close()
      }

      if(bos != null) {
        bos.close()
      }
    }

    new String(bos.toByteArray)

  }

  /**
    * @param line json字符串
    * @return 返回json 对象
    * @author youchaojiang
    */
  private def getJsonObject(line:String): JSONObject = {
    val data = new JSONObject(line)
    data
  }

  /**
    * @param str  上海电信kv获取的数据
    * @return  返回解析后的数据
    * @author youchaojiang
    */
  def parseJsonObject(str:String): String = {

    var finalValue = ""

    try {
      val result = decodeBase64 (str)
      val resultSplit = result.split("_kunyan_")
      val json = getJsonObject(resultSplit(0))
      val keyword = resultSplit(1)
      val id = json.get ("id").toString
      val value = json.get ("value").toString
      val desDe = zlibUnzip(value.replace("-<","\n"))
      val resultJson = desDe.split("\t")
      val ad = resultJson(0)
      val ts = resultJson(1)
      val host = resultJson(2).replace("\n","")
      val url = resultJson(3).replace("\n","")
      val ref = resultJson(4).replace("\n","")
      val ua = resultJson(5).replace("\n","")
      val cookie = resultJson(6).replace("\n","")

      finalValue = ts + "\t" + ad + "\t" + ua + "\t" + host +"\t"+ url + "\t" + ref + "\t" +cookie + "\t" + keyword

    } catch {
      case e:Exception  =>
        PLogger.warn("error parse JSONObject")
    }
    finalValue
  }
}
