package util

import scala.xml.{XML, Elem}

/**
  * Created by li on 16/8/29.
  */
object XMLUtil {

  /**
    * 获取xml格式的配置文件
    *
    * @param dir 配置文件所在的文件目录
    * @return
    */
  def readConfigFile(dir: String): Elem = {

    val configFile = XML.loadFile(dir)

    configFile
  }

}
