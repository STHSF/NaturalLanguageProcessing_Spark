import util.HDFSUtil

import scala.xml.XML

/**
  * Created by li on 16/7/25.
  */
object HDFSUtilTest {

  def main(args: Array[String]) {

    val configFile = XML.loadFile("/Users/li/Kunyan/NaturalLanguageProcessing/src/main/scala/util/config.xml")

    val filesystem = HDFSUtil.setHdfsConfigure(configFile)

  }


}
