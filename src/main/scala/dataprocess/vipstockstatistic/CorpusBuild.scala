package dataprocess.vipstockstatistic

import org.apache.spark.rdd.RDD

import scala.xml.XML

/**
  * Created by li on 2016/8/23.
  * 调用坤雁分词系统
  */
object CorpusBuild {

  /**
    * 配置文件初始化
    *
    * @param xmlConfPath 配置文件输入路径
    * @return 初始化后的配置文件
    * @author Li Yu
    * @note rowNum = 6
    */
  def paramInit(xmlConfPath: String): KunyanConf = {

    val kunyanConf = new KunyanConf
    val confFile = XML.loadFile(xmlConfPath)

    val kunyanHost = { confFile \ "kunyan" \ "kunyanHost" }.text
    val kunyanPort = { confFile \ "kunyan" \ "kunyanPort" }.text.toInt
    kunyanConf.set(kunyanHost, kunyanPort)

    kunyanConf
  }

  /**
    * 分词程序
    *
    * @param xmlPath 主程序输入参数
    * @author Li Yu
    * @note rownum = 6
    */
  def run(xmlPath: String, news: RDD[Array[String]]): RDD[(String, String)] = {

    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    // 配置文件初始化
    val kunyanConf = paramInit(xmlPath)

    // 调用分词系统，输出内容为URL 分词结果
    val stopWords = Array(" ")
    val corpus = news.map(row => (row(2), TextPreprocessing.process(row(3), stopWords, kunyanConf).mkString(",")))

    corpus
  }

}
