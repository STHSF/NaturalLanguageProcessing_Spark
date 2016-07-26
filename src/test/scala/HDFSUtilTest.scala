import util.HDFSUtil

/**
  * Created by li on 16/7/25.
  */
object HDFSUtilTest {

  def main(args: Array[String]) {

     HDFSUtil.createDir("/liyutest")
//    HDFSUtil.createDir("/liyutest/lll")


    HDFSUtil.createFile("/liyutest/liyu3","252435234")

//    HDFSUtil.deleteFilePath("/liyutest")


  }


}
