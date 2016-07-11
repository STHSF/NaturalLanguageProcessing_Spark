package util

import java.io.{BufferedReader, FileReader, PrintWriter}

import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2016/2/22.
  */
object FileUtil {

  /**
    * override the old one
    */
  def createFile(path: String, lines: Seq[String]): Unit = {

    val writer = new PrintWriter(path, "UTF-8")

    for (line <- lines) {
      writer.println(line)
    }
    writer.close()
  }

  def readFile(path: String): ListBuffer[String] = {

    var lines = new ListBuffer[String]()

    val br = new BufferedReader(new FileReader(path))
    try {
      var line = br.readLine()

      while (line != null) {
        lines += line
        line = br.readLine()
      }
      lines
    } finally {
      br.close()
    }
  }
}
