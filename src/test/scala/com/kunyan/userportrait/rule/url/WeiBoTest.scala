package com.kunyan.userportrait.rule.url


import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by C.J.YOU on 2016/2/25.
  */
class WeiBoTest extends FlatSpec with Matchers{

  it should "work" in {

    val url = "http://weibo.com/p/5693231739/album?"
    val cookie = "SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9W57HJOUarqwVgej9fD4JjjN5JpX5KMt; SINAGLOBAL=1526105980137.4731.1449328309020; ULV=1454777132504:74:5:1:7768739672789.377.1454777132455:1454742156804; SUHB=0JvQqgDB0RNQP2; UOR=,,login.sina.com.cn; wb_feed_unfolded_2890501720=1; wvr=6; ALF=1486313118"
    val result = WeiBo.getWeiBoIdFromCookie(cookie)
    result should be  ("2890501720")
    println(result)

  }
}
