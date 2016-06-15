package com.kunyan.userportrait.importdata.extractor

import com.kunyan.userportrait.log.PLogger
import com.kunyan.userportrait.util.StringUtil

/**
  * Created by C.J.YOU on 2016/4/27.
  * 解析电信原始用户数据信息的主类
  */
object Extractor extends Serializable {

  /**
    * 解析出用户phone，qq，微博号
    *
    * @param arr 电信数据按分隔符分隔后的数组
    * @return 用户信息元组
    */
  def extractorUserInfo(arr: Array[String]): (String, String, String) = {

    (phone(arr), qq(arr), weibo(arr))

  }

  /**
    * 获取maimai用户的id
    *
    * @param array 电信原始数据的数组形式
    * @return  （返回uid，ua，cookie）
    */
  def maimaiUserId(array: Array[String]): (String,String,String)  = {

    var info = ""
    val cookie = array(6)
    if (cookie.contains ("koa:sess=")) {
      try {
        val arr = cookie.split ("koa:sess=")
        if (arr.length > 1) {
          val value = arr(1).split(";")(0)
          if (value.nonEmpty) {
            info = StringUtil.getMaimaiUserId(value)
          }
        }
      } catch {
        case e:Exception => println("maimai userid error")
      }
    }
    (info,array(2),array(6))
  }

  /**
    * 通过其他网站提取用户手机号
    * @param array 电信数据按分隔符分隔后的数组
    * @return 用户手机号
    */
  private def phoneFromOtherWebsite(array: Array[String]): String = {

    var info = ""
    val cookie = array(6)

    // www.189.cn
    if (cookie.contains ("aactgsh111220=")) {

      val arr = cookie.split ("aactgsh111220=")

      if (arr.length > 1) {

        val value = arr (1).split (";")(0)

        if (value.nonEmpty && (value forall Character.isDigit)) {

          if (value.toLong.toString.length == 11) {

            info = value

            return info

          }
        }
      }
    }

    // 10jqka.com.cn
    if (cookie.contains ("escapename=")) {

      val arr = cookie.split ("escapename=")

      if (arr.length > 1) {

        val value = arr (1).split (";")(0)

        if (value.nonEmpty && (value forall Character.isDigit)) {

          if (value.toLong.toString.length == 11) {

            info = value

            return info

          }
        }
      }

    } else if (cookie.contains ("u_name=")) {

      val arr = cookie.split ("u_name=")

      if (arr.length > 1) {

        val value = arr (1).split (";")(0)

        if (value.nonEmpty && (value forall Character.isDigit)) {

          if (value.toLong.toString.length == 11) {

            info = value

            return info

          }
        }
      }

    } else if (cookie.contains ("ths_login_uname=")) {

      val arr = cookie.split ("ths_login_uname=")

      if (arr.length > 1) {

        val value = arr (1).split (";")(0)

        if (value.nonEmpty && (value forall Character.isDigit)) {

          if (value.toLong.toString.length == 11) {

            info = value

            return info

          }
        }
      }
    }

    // email.163.com
    if (cookie.contains ("nts_mail_user=")) {

      val arr = cookie.split("nts_mail_user=")

      if (arr.length > 1) {

        val value = arr(1).split(";")(0)

        if (value.nonEmpty && (value forall Character.isDigit)) {

          if (value.toLong.toString.length == 11) {

            info = value

            return info

          }
        }
      }
    }

    // shanghai.baixing.com
    if (cookie.contains ("tel=")) {

      val arr = cookie.split("tel=")

      if (arr.length > 1) {

        val value = arr(1).split(";")(0)

        if (value.nonEmpty && (value forall Character.isDigit)) {

          if (value.toLong.toString.length == 11) {

            info = value

            return info

          }
        }
      }
    }

    // weidian.com
    if (cookie.contains ("WD_b_tele=")) {

      val arr = cookie.split("WD_b_tele=")

      if (arr.length > 1) {

        val value = arr(1).split(";")(0)

        if (value.nonEmpty && (value forall Character.isDigit)) {

          if (value.toLong.toString.length == 11) {

            info = value

            return info

          }
        }
      }
    }

    // order.jd.com
    if (cookie.contains ("unick=")) {

      val arr = cookie.split("unick=")

      if (arr.length > 1) {

        val value = arr(1).split(";")(0)

        if (value.nonEmpty && (value forall Character.isDigit)) {

          if (value.toLong.toString.length == 11) {

            info = value

            return info

          }
        }
      }

    } else if (cookie.contains ("_pst=")) {

      val arr = cookie.split("_pst=")

      if (arr.length > 1) {

        val value = arr(1).split("_p")(0)

        if (value.nonEmpty && (value forall Character.isDigit)) {

          if (value.toLong.toString.length == 11) {

            info = value

            return info

          }
        }
      }

    } else if (cookie.contains ("pin=")) {

      val arr = cookie.split("pin=")

      if (arr.length > 1) {

        val value = arr(1).split("_p")(0)

        if (value.nonEmpty && (value forall Character.isDigit)) {

          if (value.toLong.toString.length == 11) {

            info = value

            return info

          }
        }
      }
    }

    // item.yhd.com

    if (cookie.contains ("uname=")) {

      val arr = cookie.split("uname=")

      if (arr.length > 1) {

        val value = arr(1).split("@phone")(0)

        if (value.nonEmpty && (value forall Character.isDigit)) {

          if (value.toLong.toString.length == 11) {

            info = value

            return info

          }
        }
      }
    } else if (cookie.contains ("ac=")) {

      val arr = cookie.split("ac=")

      if (arr.length > 1) {

        val value = arr(1).split(";")(0)

        if (value.nonEmpty && (value forall Character.isDigit)) {

          if (value.toLong.toString.length == 11) {

            info = value

            return info

          }
        }
      }
    }

    // renren.com
    if (cookie.contains ("ln_uact=")) {

      val arr = cookie.split("ln_uact=")

      if (arr.length > 1) {

        val value = arr(1).split(";")(0)

        if (value.nonEmpty && (value forall Character.isDigit)) {

          if (value.toLong.toString.length == 11) {

            info = value

            return info

          }
        }
      }
    }

    // eastmoney.com
    if (cookie.contains ("pu=")) {

      val arr = cookie.split("pu=")

      if (arr.length > 1) {

        val value = arr(1).split(";")(0)

        if (value.nonEmpty && (value forall Character.isDigit)) {

          if (value.toLong.toString.length == 11) {

            info = value

            return info

          }
        }
      }
    }

    // kejiqi.com
    if (cookie.contains ("DfR_guest_mobile=")) {

      val arr = cookie.split("DfR_guest_mobile=")

      if (arr.length > 1) {

        val value = arr(1).split(";")(0)

        if (value.nonEmpty && (value forall Character.isDigit)) {

          if (value.toLong.toString.length == 11) {

            info = value

            return info

          }
        }
      }
    }

    // youtx.com
    if (cookie.contains ("autousername=")) {

      val arr = cookie.split("autousername=")

      if (arr.length > 1) {

        val value = arr(1).split(";")(0)

        if (value.nonEmpty && (value forall Character.isDigit)) {

          if (value.toLong.toString.length == 11) {

            info = value

            return info

          }
        }
      }
    }

    // jiayuan.com
    if (cookie.contains ("save_jy_login_name=")) {

      val arr = cookie.split("save_jy_login_name=")

      if (arr.length > 1) {

        val value = arr(1).split(";")(0)

        if (value.nonEmpty && (value forall Character.isDigit)) {

          if (value.toLong.toString.length == 11) {

            info = value

            return info

          }
        }
      }
    }

    // vip.com
    if (cookie.contains ("login_username=")) {

      val arr = cookie.split("login_username=")

      if (arr.length > 1) {

        val value = arr(1).split(";")(0)

        if (value.nonEmpty && (value forall Character.isDigit)) {

          if (value.toLong.toString.length == 11) {

            info = value

            return info

          }
        }
      }
    }

    // music.migu.cn
    if (cookie.contains ("USER_NAME=")) {

      val arr = cookie.split("USER_NAME=")

      if (arr.length > 1) {

        val value = arr(1).split(";")(0)

        if (value.nonEmpty && (value forall Character.isDigit)) {

          if (value.toLong.toString.length == 11) {

            info = value

            return info

          }
        }
      }
    } else if (cookie.contains ("USER_MOBILE=")) {

      val arr = cookie.split("USER_MOBILE=")

      if (arr.length > 1) {

        val value = arr(1).split(";")(0)

        if (value.nonEmpty && (value forall Character.isDigit)) {

          if (value.toLong.toString.length == 11) {

            info = value

            return info

          }
        }
      }
    }

    info

  }

  /**
    * 提取用户手机号
    * @param array 电信数据按分隔符分隔后的数组
    * @return 用户手机号
    */
  private def phone(array: Array[String]): String = {

    var info = ""

    try {

      val cookie = array(6)

      if (cookie.contains("cSaveState=")) {

        val template = "(?<=cSaveState=)\\d{11}".r
        val resultPhone = template.findAllMatchIn (cookie)

        resultPhone.foreach (x => {

          info = x.toString

          return info

        })
      } else if (cookie.contains("un=")) {

        val arr = cookie.split("un=")

        if (arr.length > 1) {

          val value = arr (1).split(";")(0)

          if (value forall Character.isDigit) {

            info = value

            return info

          }
        }
      }

      if (cookie.contains ("idsLoginUserIdLastTime=")) {

        val arr = cookie.split("idsLoginUserIdLastTime=")

        if (arr.length > 1) {

          val value = arr(1).split(";")(0)

          if (value forall Character.isDigit) {

            info = value

            return info

          }
        }
      }

      // 点评获取用户手机
      if (cookie.contains("ua=")) {

        try {

          val arr = cookie.split("ua=")

          if (arr.length > 1) {

            val value = arr(1).split(";")(0)

            if (value.nonEmpty && (value forall Character.isDigit)) {

              if (value.toLong.toString.length == 11) {

                info = value

                return info

              }
            }
          }
        } catch {
          case e:Exception => println("dian ping phone error")
        }
      }

      // kfc 获取用户手机
      if (cookie.contains("yum_ueserInfo=")) {

        val arr = cookie.split("yum_ueserInfo=")

        if (arr.length > 1) {

          val value = arr(1).split(";")(0)

          if (value.nonEmpty && (value forall Character.isDigit)) {

            if (value.toLong.toString.length == 11) {

              info = value

              return info

            }
          }
        }
      }
      // elong.com
      if (cookie.contains ("member=")) {

        val arr = cookie.split("member=")

        if (arr.length > 1) {

          val value = arr(1).split(";")(0)

          if (value.nonEmpty && (value forall Character.isDigit)) {

            if (value.toLong.toString.length == 11) {

              info = value

              return info

            }
          }
        }
      }

      // lzhe.com
      if (cookie.contains ("LOGIN_NAME=")) {

        val arr = cookie.split("LOGIN_NAME=")

        if (arr.length > 1) {

          val value = arr(1).split(";")(0)

          if (value.nonEmpty && (value forall Character.isDigit)) {

            if (value.toLong.toString.length == 11) {

              info = value

              return info

            }
          }
        }
      } else if (cookie.contains ("U_M=")) {

        val arr = cookie.split("U_M=")

        if (arr.length > 1) {

          val value = arr(1).split(";")(0)

          if (value.nonEmpty && (value forall Character.isDigit)) {

            if (value.toLong.toString.length == 11) {

              info = value

              return info

            }
          }
        }
      }

      // esf.fangdd.com
      if (cookie.contains ("phone=")) {

        val arr = cookie.split("phone=")

        if (arr.length > 1) {

          val value = arr(1).split(";")(0)

          if (value.nonEmpty && (value forall Character.isDigit)) {

            if (value.toLong.toString.length == 11) {

              info = value

              return info

            }
          }
        }
      }

      // yougou.com
      if (cookie.contains ("belle_username=")) {

        val arr = cookie.split("belle_username=")

        if (arr.length > 1) {

          val value = arr(1).split(";")(0)

          if (value.nonEmpty && (value forall Character.isDigit)) {

            if (value.toLong.toString.length == 11) {

              info = value

              return info

            }
          }
        }
      }
    } catch {

      case e:Exception => PLogger.warn("phone extractor error")

    }

    info = phoneFromOtherWebsite(array)

    info

  }

  /**
    * 获取用户微博号
    *
    * @param array  电信数据按分隔符分隔后的数组
    * @return 用户微博号
    */
  private def weibo(array: Array[String]): String = {

    var info =""
    val cookie = array(6)

    if (cookie.contains("SUS=SID-")) {

      val info = cookie.split("SUS=SID-")(1).split("-")(0)

      return  info

    } else  if (cookie.contains("SUP=")) {

      try {

        val cookieValue = StringUtil.urlDecode(cookie)
        val name = cookieValue.indexOf("uid=")

        if (name != -1) {

          val d = cookieValue.substring(name + 4).split("&")(0)

          info = d.toString

          return  info

        }
      } catch {

        case e:Exception => PLogger.warn("weibo id url decode error")

      }
    } else  if(cookie.contains("wb_feed_unfolded_")) {

      val info = cookie.split ("wb_feed_unfolded_")(1).split ("=")(0)

      return  info

    }

    info

  }

  /**
    * 获取用户qq号
    *
    * @param array 电信数据按分隔符分隔后的数组
    * @return 用户qq
    */
  private def qq(array: Array[String]): String = {

    val cookie = array(6)
    var info =""

    if (cookie.contains("SUP=")) {

      try {

        val name = cookie.indexOf("name=")

        if (name != -1) {

          val d = cookie.substring(name + 5).split(";")(0)
          val decode = StringUtil.urlDecode(d).split("&")(0)

          if (decode.contains("%40qq.com")){

            val tempInfo = decode.replace("%40qq.com", "")

            if(tempInfo forall Character.isDigit){

              return tempInfo

            }
          }
        }
      } catch {

        case e:Exception => PLogger.warn("qq url decode error")

      }
    } else if (cookie.contains("o_cookie=")) {

      val arr = cookie.split("o_cookie=")

      if (arr.length > 1) {

        if(arr(1).contains(";")){

          info = arr(1).split(";")(0)

          return  info

        }else{

          info = arr(1)

          return  info

        }
      }
    } else if (cookie.contains("qzone_check=")) {

      val arr = cookie.split("qzone_check=")

      if (arr.length > 1) {

        info = arr(1).split("_")(0)

        return  info

      }
    } else if(cookie.contains("pt2gguin=o")){

      val arr = cookie.split("pt2gguin=o")

      if (arr.length > 1) {

        if(arr(1).contains(";")){

          info = arr(1).split(";")(0)

        }else{

          info = arr(1)

        }
        if(info.nonEmpty){

          info = info.toLong.toString

          return  info

        }
      }
    }
    // dian ping
    if(cookie.contains("ua=")){

      try {

        val arr = cookie.split("ua=")

        if (arr.length > 1) {

          val value = arr(1).split(";")(0)

          if (value.contains("%40qq.com")) {

            val tempInfo = value.replace("%40qq.com", "")

            if (tempInfo.nonEmpty && (tempInfo forall Character.isDigit)) {

              info = tempInfo.toLong.toString

              return info

            }
          }
        }
      } catch {

        case e:Exception => PLogger.warn("dian ping qq error")

      }
    }

    /**
      * kfc 获取用户qq
      */
    if(cookie.contains("yum_ueserInfo=")) {

      try {

        val arr = cookie.split("yum_ueserInfo=")

        if (arr.length > 1) {

          val value = arr(1).split(";")(0)

          if (value.contains("%40qq.com")) {

            val tempInfo = value.replace("%40qq.com", "")

            if (tempInfo.nonEmpty && (tempInfo forall Character.isDigit)) {

              info = tempInfo.toLong.toString

              return info

            }
          }
        }
      } catch {

        case e:Exception => PLogger.warn("kfc qq error")

      }
    }
    info
  }

  /**
    * 获取用户邮箱
    *
    * @param array 电信数据按分隔符分隔后的数组
    * @return 用户邮箱
    */
  private  def email(array: Array[String]): String = {

    val cookie = array(6)
    var info =""

    if (cookie.contains("cSaveState=")) {

      val template = "(?<=cSaveState=)\\w+@\\w+\\.com".r
      val resultPhone  =  template.findAllMatchIn(cookie)

      resultPhone.foreach(x => {

        info = x.toString()

        return  info

      })
    } else if (cookie.contains("un=")) {

      val templateUser = "(?<=un=)\\w+@\\w+\\.com".r
      val userPhone  =  templateUser.findAllMatchIn(cookie)

      userPhone.foreach(x => {

        info = x.toString()

        return  info

      })
    }
    if(cookie.contains("idsLoginUserIdLastTime=")) {

      val template = "(?<=idsLoginUserIdLastTime=)\\w+%40\\w+\\.com".r
      val resultPhone  =  template.findAllMatchIn(cookie)

      resultPhone.foreach(x => {

        info = x.toString().replace("%40","@")

        return  info

      })
    }
    info
  }
}
