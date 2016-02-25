import com.kunyan.userportrait.platform.{WeiBo, ZhiHu}
import org.scalatest.{Matchers, FlatSpec}

/**
  * Created by C.J.YOU on 2016/2/25.
  */
class WeiBo extends FlatSpec with Matchers{
  it should "work" in {

    val url = "http://api.weibo.cn/2/account/getcookie?v_f=2&s=73ea9bd3&gsid=4u9523853hZMTMTyXh9Nt9z5S68&c=android&wm=5091_0008&ua=Xiaomi-MI%202C__weibo__6.0.0__android__android4.1.1&oldwm=9975_0001&aid=01AjUb009sU-_uao6iQQC6lnDNB6Cz3iSSRCkGlj_95kV4ZTM.&from=1060095010&networktype=wifi&lang=zh_CN&skin=com.sina.weibo.pinkallure&i=d1e5e5a&sflag=1"
    val regx = "(?<=from=)\\d{8,10}(?=)".r
    val result = regx.findAllMatchIn(url)
    result.foreach(println)
  }
}
