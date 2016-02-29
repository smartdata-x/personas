package com.kunyan.userportrait.rule.url

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by C.J.YOU on 2016/2/26.
  */
class QzoneTest extends FlatSpec with Matchers{
  it should "work" in {

    val url = "http://user.qzone.qq.com/729766011/infocenter?ptsig=W3tZ9l-zN01y-Qft8utZCrkRTI2OeRa2IsFGBYIYfyc_"
    val cookies = "hasShowWeiyun974572550=1; RK=KGsWp41nWo; pgv_pvi=8170322357; __Q_w_s__QZN_TodoMsgCnt=1; cpu_performance_v8=0; pgv_pvid=1354195922; qz_screen=1366x768; QZ_FE_WEBP_SUPPORT=1; appCanvasGoldCouponBubble=true; Loading=Yes; pt2gguin=o0974572550; uin=o0974572550; skey=@A59w9XNeF; ptisp=ctc; qzone_check=974572550_1454843993; ptcz=959e2706fcee7220a465f920cc31d788f44a01dbe03750b6912ffc761c8f0208"
    // val phone = PhoneUtil.getPhone(url)
    val QQ = Qzone.getQQFromCookies(cookies)
    println(QQ)

  }
}
