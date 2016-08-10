package telecomdataprocessing.util

import com.ibm.icu.text.CharsetDetector
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.xml.{Elem, XML}

/**
  * Created by li on 16/7/7.
  */
object HBaseUtil {

  /**
    * 识别字符编码
    *
    * @param html 地址编码
    * @return 字符编码
    */
  def judgeChaser(html: Array[Byte]): String = {

    val icu4j = new CharsetDetector()
    icu4j.setText(html)
    val encoding = icu4j.detect()

    encoding.getName
  }

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
    * 获取hbase配置内容,并且初始化hbase配置
    *
    * @param configFile hbase配置文件
    * @return
    * @return Li Yu
    * @note rowNum: 7
    */
  def setHBaseConfigure(configFile: Elem): Configuration = {

    val rootDir = (configFile \ "hbase" \ "rootDir").text
    val ip = (configFile \ "hbase" \ "ip").text

    // 初始化配置
    val configuration = HBaseConfiguration.create()
    configuration.set("hbase.rootdir", rootDir)
    configuration.set("hbase.zookeeper.quorum", ip)

    configuration
  }

  /**
    * 获取hbase中的内容
    *
    * @param sc SparkContext
    * @param confDir 配置文件所在的文件夹
    * @author Li Yu
    * @note rowNum: 7
    */
  def getHBaseConf(sc: SparkContext, confDir: String, tableName: String) : RDD[(ImmutableBytesWritable, Result)] = {

    val configFile = HBaseUtil.readConfigFile(confDir)
    val configuration = HBaseUtil.setHBaseConfigure(configFile)

    configuration.set(TableInputFormat.INPUT_TABLE, tableName)

    // 使用Hadoop api来创建一个RDD
    val hBaseRDD = sc.newAPIHadoopRDD(configuration,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    hBaseRDD
  }

}
