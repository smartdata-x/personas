package com.kunyan.userportrait.rule.crawl

import com.kunyan.userportrait.config.PlatformConfig

/**
  * Created by yangshuai on 2016/2/27.
  */
object CrawlableURLFilter {

  def filter(url: String): (Int, String) = {
    eleMe(url)
  }

  def eleMe(url: String): (Int, String) = {

    if(url.contains("http://www.ele.me/profile/address")){
      return (PlatformConfig.PLATFORM_ELEME, "address")
    }

    null
  }

}
