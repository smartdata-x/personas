package com.kunyan.userportrait.rule.url


import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by C.J.YOU on 2016/2/25.
  */
class WeiBoTest extends FlatSpec with Matchers{

  it should "work" in {

    val url = "http://weibo.com/p/5693231739/album?"
    val cookie = "YF-Ugrow-G0=9642b0b34b4c0d569ed7a372f8823a8e; YF-V5-G0=8d795ebe002ad1309b7c59a48532ef7d; login_sid_t=ea4832e96bd8a835a584ebe30348097b; SUS=SID-3478796740-1454820403-GZ-ofhuj-51c14870d5d7914c74c4f6c89a1ea368; SUE=es%3D2d433e999c8f14280dcd743b379b9d22%26ev%3Dv1%26es2%3D30dd154755a9aac702eb11c27bf994b6%26rs0%3DOWufCFwRWiXej9P6zsL%252BXbJty7OzJZ2Heq%252Bc2Fjpl7mCqhqL2alb7fSIeoJF4GQWjgyxsrTd%252FmSOppTBaDDTjQucHnMKc1oobf%252F%252BcMKR%252FBiIc7pPul3K05SPpju6KLprQNKxYlU6wtr9fIcIT5pHFEUuc7zopBHZBvVgB5ZWz6o%253D%26rv%3D0; SUP=cv%3D1%26bt%3D1454820403%26et%3D1454906803%26d%3Dc909%26i%3Da368%26us%3D1%26vf%3D0%26vt%3D0%26ac%3D18%26st%3D0%26uid%3D3478796740%26name%3D15892741917%26nick%3D%25E7%2594%25A8%25E6%2588%25B73478796740%26fmp%3D%26lcp%3D2016-01-18%252021%253A17%253A32; SUB=_2A257srxjDeRxGeVK7FoW-SjLzzyIHXVYyaqrrDV8PUJbuNBeLW_YkW9LHetbKluc4cczt7sZBEQhK64TjFIUcw..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WW6n3lQQnOBWS9w98SaujyU5JpX5K2t; SUHB=0HThfdC9WVYIvL; SSOLoginState=1454820403; wvr=6"
    val result = WeiBo.getWeiBoIdFromCookie(cookie)
    result should be  ("3478796740")

  }
}
