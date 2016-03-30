package com.kunyan.userportrait.rule.url

import com.kunyan.userportrait.db.HbaseUtil
import org.apache.hadoop.hbase.util.Bytes
import org.jsoup.Jsoup

/**
  * Created by yangshuai on 2016/3/16.
  */
object Test extends App {

  val result = HbaseUtil.getDataByRowkey("weibo_step1", "fc88033da1d7459a3dfb6daa225d0850")
  val contentBytes = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("content"))
  val html = Jsoup.parse(new String(contentBytes)).toString

  println(html)

  if (html.contains("$CONFIG['page_id']='")) {
    val arr = html.split("CONFIG\\['page_id'\\]='")
    if ( arr.length > 1) {
      val result = arr(1).split("';")(0)
      println(result)
    } else {
      println(arr(0))
    }
  }

}
