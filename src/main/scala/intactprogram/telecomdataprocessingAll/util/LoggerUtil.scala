package telecomdataprocessingAll.util

import org.apache.log4j.{BasicConfigurator, Logger}

/**
  * 写Log操作
  */
object LoggerUtil {

  var logger = Logger.getLogger("TelecomData_Processing")
  BasicConfigurator.configure()
//  PropertyConfigurator.configure("/home/mlearning/tdt/conf/log4j.properties")

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
