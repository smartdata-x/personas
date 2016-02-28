package com.kunyan.userportrait.rule.url

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by C.J.YOU on 2016/2/26.
  */
class SuNingTest extends FlatSpec with Matchers{
  it should "work" in {

    val url = "http://click.suning.cn/sa/ajaxClick.gif?_snmk=14548227562927231|145481931416471867|id%20undefined|review_dp_fbpj_fbpj|%E5%8F%91%E8%A1%A8%E8%AF%84%E4%BB%B7|http%3A%2F%2Freview.suning.com%2Forder_cmmdty_review.do%3ForderId%3D10026245936%26shopId%3DSN_001&_snme=&_type=&_cId=undefined&_sid=-&urlPattern=&vid=1439696230715313&lu=15921452710&sid=145481922761314395&mid=6006086631&ls=2&title=&iId=log_1454822756295"
    val cookies = "X193bXY9MTQ0ODkzNzQ1Mi4xOyBfY3VzdG9tSWQ9czZycmUyZmYwMjUxOyBfZ2E9R0ExLjIuMTUwNzA2MTQ5MC4xNDQ1MDc4MjgyOyBfc25tYT0xJTdDMTQ0NTA3ODI3ODg1NjI0NTY0JTdDMTQ0NTA3ODI3ODg1NiU3QzE0NTQ4ODQ5ODQwMTklN0MxNDU0ODg1MDI4NTQ3JTdDOTkwJTdDMTQ5OyBhdXRoSWQ9c2k1N0FGMDcxNTM0MTBDQ0E5NjY3MTAxQTAzNjVBM0MwNzsgY2l0eUNvZGU9MDIxOyBjaXR5SWQ9OTI2NDsgY3VzdG5vPTYwMTM5MDY5ODE7IGRpc3RyaWN0Q29kZT0xODsgZGlzdHJpY3RJZD0xMjEzMDsgaWRzTG9naW5Vc2VySWRMYXN0VGltZT0xMzUyNDYzNDI3NzsgaW5kZXhfdjM9MTsgbG9nb25TdGF0dXM9MDsgbXRpc0FiVGVzdD1BOyBuaWNrPSVFNSU5NCU5MCVFNSVCMCU4RiVFNSVBNiVCOTExMS4uLjsgbmljazI9JUU1JTk0JTkwJUU1JUIwJThGJUU1JUE2JUI5MTExLi4uOyBzd2l0Y2hDc2M9TVRDOyB0b3RhbFByb2RRdHk9NjsgdG90YWxQcm9kUXR5djM9NzsgV0NfUEVSU0lTVEVOVD12WEM2JTJiJTJiOElYbjFrM0ZhbVlFanBjamcwdEtVJTNkJTBhJTNiMjAxNiUyZDAxJTJkMTcrMTAlM2EyNyUzYTQxJTJlODg3JTVmMTQ1Mjk5NzY2MTg4NiUyZDIyNjA3NCU1ZjEwMDUyOyBXQ19TRVJWRVI9NQ"
    // val phone = PhoneUtil.getPhone(url)
    val phone = SuNing.getPhoneFromCookies(cookies)
    println(phone)

  }
}
