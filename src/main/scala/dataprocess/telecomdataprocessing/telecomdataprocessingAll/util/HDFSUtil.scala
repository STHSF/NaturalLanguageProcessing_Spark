package telecomdataprocessingAll.util

import java.text.SimpleDateFormat

/**
  * Created by li on 16/7/25.
  */
object HDFSUtil {


  def main(args: Array[String]) {
    val dataFormat = new SimpleDateFormat("yyyy-MM-dd")
    val startTime = dataFormat.parse("2012-12-12")
    val startTimeStamp = startTime.getTime
    val stopTimeStamp = startTime.getTime - 24 * 60 * 60 * 1000 -1


    println(startTimeStamp, stopTimeStamp)
  }



}
