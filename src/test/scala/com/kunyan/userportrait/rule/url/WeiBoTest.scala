package com.kunyan.userportrait.rule.url


import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by C.J.YOU on 2016/2/25.
  */
class WeiBoTest extends FlatSpec with Matchers{

  it should "work" in {

    val url = "http://weibo.com/p/5693231739/album?"
    val cookie = "SINAGLOBAL=1709996301215.142.1454826118560; ULV=1454834109151:2:2:2:7366948167327.791.1454834109146:1454826118572; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9Whn7vvAZTGVg3jme7ECBXIf5JpX5K2t; SUHB=0cS29566GlR_S4; ALF=1455438931; un=wang_yuan_xiang@hotmail.com; UOR=www.kdslife.com,widget.weibo.com,spr_qdhz_bd_baidusmt_weibo_s; wvr=6"
    val result = WeiBo.getEmailFromCookie(cookie)
    result should be  ("wang_yuan_xiang@hotmail.com")
    println(result)

  }
}
