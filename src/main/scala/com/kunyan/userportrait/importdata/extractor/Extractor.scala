package com.kunyan.userportrait.importdata.extractor

import com.kunyan.userportrait.log.PLogger
import com.kunyan.userportrait.util.StringUtil
import scala.util.control.Breaks._

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
    * @return （返回uid，ua，cookie）
    */
  def maimaiUserId(array: Array[String]): (String, String, String) = {

    var info = ""
    val cookie = array(6)

    if (cookie.contains("koa:sess=")) {

      try {

        val arr = cookie.split("koa:sess=")

        if (arr.length > 1) {

          val value = arr(1).split(";")(0)

          if (value.nonEmpty) {
            info = StringUtil.getMaimaiUserId(value)
          }
        }
      } catch {

        case e: Exception => PLogger.warn("maimai userid error")

      }
    }

    (info, array(2), array(6))

  }




  /**
    * 提取用户手机号
    *
    * @param array 电信数据按分隔符分隔后的数组
    * @return 用户手机号
    */
  private def phone(array: Array[String]): String = {

    var info = ""
    val cookie = array(6)

    val specialSigns = Map(
      ("aactgsh111220=", ";"), ("escapename=", ";"), ("u_name=", ";"), ("ths_login_uname=", ";"), ("save_jy_login_name=", ";"),
      ("LOGIN_NAME=", ";"), ("U_M=", ";"), ("phone=", ";"), ("belle_username=", ";"),
      ("unick=", ";"), ("_pst=", "_p"), ("pin=", "_p"),
      ("nts_mail_user=", ";"), ("tel=", ";"), ("pu=", ";"), ("DfR_guest_mobile=", ";"), ("autousername=", ";"),
      ("login_username=", ";"), ("USER_NAME=", ";"), ("USER_MOBILE=", ";"),
      ("WD_b_tele=", ";"), ("uname=", "@phone"), ("ac=", ";"), ("ln_uact=", ";"),
      ("ua=", ";"), ("yum_ueserInfo=", ";"), ("member=", ";"),("cSaveState=",";"),("un=",";"),("idsLoginUserIdLastTime=",";")
    )

    try {
    breakable {

      specialSigns.foreach(specialSign => {
        info = getPhoneFromWeibo(cookie, specialSign)
        if (info != "")
          break
      })

    }

    } catch {

      case e: Exception =>
        PLogger.exception(e)
        PLogger.warn("phone extractor error")

    }


    info

  }

  def getPhoneFromWeibo(cookie: String, specialSign: (String, String)): String = {

    var info = ""

    if (cookie.contains(specialSign._1)) {

      val arr = cookie.split(specialSign._1)

      if (arr.length > 1) {

        val value = arr(1).split(specialSign._2)(0)

        if (value.nonEmpty && (value forall Character.isDigit)) {

          if (value.toLong.toString.length == 11)
            info = value

        }
      }
    }

    info
  }

  /**
    * 获取用户微博号
    *
    * @param array 电信数据按分隔符分隔后的数组
    * @return 用户微博号
    */
  private def weibo(array: Array[String]): String = {

    var info = ""
    val cookie = array(6)

    if (cookie.contains("SUS=SID-")) {

      val info = cookie.split("SUS=SID-")(1).split("-")(0)

      return info

    }

    if (cookie.contains("wb_feed_unfolded_")) {

      val info = cookie.split("wb_feed_unfolded_")(1).split("=")(0)

      return info

    }

    if (cookie.contains("SUP=")) {

      try {

        val cookieValue = StringUtil.urlDecode(cookie)
        val name = cookieValue.indexOf("uid=")

        if (name != -1) {

          val d = cookieValue.substring(name + 4).split("&")(0)

          info = d.toString

          return info

        }
      } catch {

        case e: Exception => PLogger.warn("weibo id url decode error")

      }
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
    var info = ""

    if (cookie.contains("SUP=")) {

      try {

        val name = cookie.indexOf("name=")

        if (name != -1) {

          val d = cookie.substring(name + 5).split(";")(0)
          val decode = StringUtil.urlDecode(d).split("&")(0)

          if (decode.contains("%40qq.com")) {

            val tempInfo = decode.replace("%40qq.com", "")

            if (tempInfo forall Character.isDigit) {

              return tempInfo

            }
          }
        }
      } catch {

        case e: Exception => PLogger.warn("qq url decode error")

      }

    } else if (cookie.contains("o_cookie=")) {

      val arr = cookie.split("o_cookie=")

      if (arr.length > 1) {

        if (arr(1).contains(";")) {

          info = arr(1).split(";")(0)

          return info

        } else {

          info = arr(1)

          return info

        }
      }

    } else if (cookie.contains("qzone_check=")) {

      val arr = cookie.split("qzone_check=")

      if (arr.length > 1) {

        info = arr(1).split("_")(0)

        return info

      }

    } else if (cookie.contains("pt2gguin=o")) {

      val arr = cookie.split("pt2gguin=o")

      if (arr.length > 1) {

        if (arr(1).contains(";")) {

          info = arr(1).split(";")(0)

        } else {

          info = arr(1)

        }
        if (info.nonEmpty) {

          info = info.toLong.toString

          return info

        }
      }
    }
    // dian ping
    if (cookie.contains("ua=")) {

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

        case e: Exception => PLogger.warn("dian ping qq error")

      }
    }

    // kfc 获取用户qq
    if (cookie.contains("yum_ueserInfo=")) {

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

        case e: Exception => PLogger.warn("kfc qq error")

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
  private def email(array: Array[String]): String = {

    val cookie = array(6)
    var info = ""

    if (cookie.contains("cSaveState=")) {

      val template = "(?<=cSaveState=)\\w+@\\w+\\.com".r
      val resultPhone = template.findAllMatchIn(cookie)

      resultPhone.foreach(x => {

        info = x.toString()

        return info

      })
    } else if (cookie.contains("un=")) {

      val templateUser = "(?<=un=)\\w+@\\w+\\.com".r
      val userPhone = templateUser.findAllMatchIn(cookie)

      userPhone.foreach(x => {

        info = x.toString()

        return info

      })
    }
    if (cookie.contains("idsLoginUserIdLastTime=")) {

      val template = "(?<=idsLoginUserIdLastTime=)\\w+%40\\w+\\.com".r
      val resultPhone = template.findAllMatchIn(cookie)

      resultPhone.foreach(x => {

        info = x.toString().replace("%40", "@")

        return info

      })
    }
    info
  }
}
