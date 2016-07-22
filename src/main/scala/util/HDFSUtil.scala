package util


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}

/**
  * Created by li on 16/7/12.
  */
object HDFSUtil {

  // 配置环境说明

  val conf = new Configuration()

  val hdfs = FileSystem.get(conf)//获得HDFS的FileSystem对象




  def readFromHDFS(): Unit ={

  }




  def Write2HDFS(dir: String , data: Array[String]): Unit = {

    val path = new Path(dir)

    val dd = hdfs.create(path)

    data.foreach(x => dd.writeUTF(x))


    dd.close()

  }




}
