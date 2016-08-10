package util


import java.io.{BufferedInputStream, FileInputStream}
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.xml.{XML, Elem}

/**
  * Created by li on 16/7/12.
  */
object HDFSUtil {

  // 配置环境说明
  private val conf = new Configuration()
  conf.addResource(new Path("/opt/hadoop-0.20.0/conf/core-site.xml"))
  conf.addResource(new Path("/opt/hadoop-0.20.0/conf/hdfs-site.xml"))

  private val fileSystem = FileSystem.get(new URI("hdfs://222.73.57.12:9000"),conf)//获得HDFS的FileSystem对象

  /**
    * 获取xml格式的配置文件
    *
    * @param dir 配置文件所在的文件目录
    * @return
    * @return Li Yu
    * @note rowNum: 2
    */
  def readConfigFile(dir: String): Elem = {

    val configFile = XML.loadFile(dir)

    configFile
  }

  /**
    * 生成fileSystem实例
    * @param configFile 配置文件
    * @return
    * @return Li Yu
    * @note rowNum: 7
    */
  def setHdfsConfigure(configFile: Elem): FileSystem = {

    val conf = new Configuration()
    conf.addResource(new Path("/opt/hadoop-0.20.0/conf/core-site.xml"))
    conf.addResource(new Path("/opt/hadoop-0.20.0/conf/hdfs-site.xml"))
    val rootDir = (configFile \ "hdfs" \ "rootDir").text

    val fileSystem = FileSystem.get(new URI(rootDir),conf)//获得HDFS的FileSystem对象

    fileSystem
  }


  /**
    * 删除文件
    *
    * @param fileName
    */
  private def deleteFile(fileName: String): Unit = {

    val path = new Path(fileName)
    val isExists = fileSystem.exists(path)
    if (isExists) {

      val isDel = fileSystem.delete(path, true)
      System.out.println(fileName + "  delete? \t" + isDel)

    } else {

      System.out.println(fileName + "  exist? \t" + isExists)

    }
  }

  /**
    * 删除文件目录
    *
    * @param dir
    * @return
    */
  private def deleteFilePath(dir: String): Boolean = {

    val path = new Path(dir)

    if (fileSystem.isDirectory(path)) {

      val children = fileSystem.listStatus(path)

      //递归删除目录中的子目录下
      for (i <- 0 until children.length) {

        val success = deleteFilePath(children(i).getPath.toString)

        if (! success){
          return false
        }
      }
    }
    // 目录此时为空，可以删除
    fileSystem.delete(path, true)
  }

  /**
    * 创建文件目录
    *
    * @param str
    */
  private def createDir (str: String): Unit = {

    val path = new Path(str)

    fileSystem.mkdirs(path)

  }

  /**
    * 创建文件
    *
    * @param fileName
    * @param fileContent
    */
  private def createFile(fileName: String, fileContent: String): Unit = {

    val path = new Path(fileName)
    val bytes = fileContent.getBytes("UTF-8")

    val output = fileSystem.create(path, true)

    output.write(bytes)

    output.close()
  }


  private def createFile2(fileName: String, fileContent: String): Unit = {

    val path = new Path(fileName)
    val bytes = fileContent.getBytes("UTF-8")

    val output = fileSystem.create(path, true)

//    val output = fileSystem.append(path)
    val in = new BufferedInputStream(new FileInputStream(fileContent))
//    IOUtils.copyBytes(in, output, 4096, true)
    output.write(bytes)

    output.close()
  }

  /**
    * 向hdfs上写文件内容
    *
    * @param dir
    * @param fileName
    * @param fileContent
    */
  def Write2HDFS(dir: String, fileName: String, fileContent: String): Unit = {

    val path = new Path(dir + fileName)

    if (fileSystem.exists(path)) {

      if (deleteFilePath(dir + fileName)) {

        createDir(dir)
      }

    } else {

      createDir(dir)
    }

    val file = dir + fileName
    createFile(file, fileContent)

    fileSystem.close()

  }


  /**
    * 向hdfs文件中添加文件内容
    *
    * @param dir
    * @param fileName
    * @param fileContent
    */
  def append2HDFS(dir: String, fileName: String, fileContent: String): Unit ={

  }

}
