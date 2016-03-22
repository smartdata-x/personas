package com.kunyan.userportrait.log

import org.apache.log4j.{PropertyConfigurator, BasicConfigurator, Logger}

/**
  * Created by yangshuai on 2016/3/3.
  */
object PLogger {

  val logger = Logger.getLogger("PERSONAS")
  BasicConfigurator.configure()
  PropertyConfigurator.configure("/home/portrait/conf/log4j.properties")

  def exception(e: Exception) = {
    logger.error(e.printStackTrace())
  }

  def error(msg: String): Unit = {
    logger.error(msg)
  }

  def warn(msg: String): Unit = {
    logger.warn(msg)
  }

  def info(msg: String): Unit = {
    logger.info(msg)
  }

  def debug(msg: String): Unit = {
    logger.debug(msg)
  }
}
