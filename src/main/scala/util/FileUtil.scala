package util

import java.io.{File, BufferedReader, FileReader, PrintWriter}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * Created by li on 2016/2/22.
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

  /** 将结果保存到本地,将每小时数据保存为一个txt文件,一天的数据保存在一个文件夹里.
    *
    * @param dir 文件保存的目录
    * @param result
    * @author Li Yu
    */
  def saveAsTextFile(dir: String, result: Array[(String, Double)]): Unit ={

    val day = TimeUtil.getDay
    val hour = TimeUtil.getCurrentHour

    val writer = new PrintWriter(new File(dir +"%s".format(day) + "-" + "%s".format(hour) + ".txt"))

    for (line <- result) {

      writer.write(line._1 + "\t" + line._2 + "\n")

    }

    writer.close()
  }

  /**
    * 读取当前时间前一个小时的数据,读取本地文件中的结果.
    *
    * @param dir 数据保存的目录
    * @return
    */
  def readFromFile(dir: String): Array[(String, Double)] ={

    val date = TimeUtil.getPreHourStr

    val temp = Source.fromFile(dir + "%s".format(date) + ".txt" )

    val res = new mutable.ArrayBuffer[(String, Double)]
    temp.getLines().foreach(
      line =>{
        val temp = line.split("\t")
        res.+=((temp(0), temp(1).toDouble))
      }
    )
    res.toArray
  }

}
