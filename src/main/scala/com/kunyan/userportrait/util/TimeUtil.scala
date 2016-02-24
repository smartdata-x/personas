package com.kunyan.userportrait.util

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
  * Created by C.J.YOU on 2016/2/24.
  */
object TimeUtil {

  def getDay: String = {
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val date: String = sdf.format(new Date)
    date
  }

  def getCurrentHour: Int = {
    val calendar = Calendar.getInstance
    calendar.setTime(new Date)
    calendar.get(Calendar.HOUR_OF_DAY)
  }

}
