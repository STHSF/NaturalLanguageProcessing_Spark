package telecomdataprocessing.util

import com.ibm.icu.text.CharsetDetector
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Scan, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.xml.{Elem, XML}

/**
  * Created by li on 16/7/7.
  */
object HBaseUtil {

  /**
    * 获取Hbase配置
    *
    * @param rootDir hbase 根目录
    * @param ips 集群节点
    * @return hbaseConf
    * @author liumiao
    * @note rowNum  6
    */
  def getHbaseConf(rootDir: String, ips: String): Configuration = {

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.rootdir", rootDir)
    hbaseConf.set("hbase.zookeeper.quorum", ips)

    hbaseConf
  }


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
    //    configuration.set("hbase.zookeeper.property.cilentport", "2181") //设置zookeeper client 端口configuration
    //    configuration.set("hbase.zookeeper.quorum", "localhost") //设置zookeeper quorum
    //    configuration.set("hbasemaster", "localhost:60000") //设置hbase master
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
//  def getHBaseConf(sc: SparkContext, confDir: String, timeRange: (Long, Long), tableName: String) : RDD[(ImmutableBytesWritable, Result)] = {
//
//    val configFile = HBaseUtil.readConfigFile(confDir)
//    val configuration = HBaseUtil.setHBaseConfigure(configFile)
//
//    // 读取HBase中的文件
//    val scan = new Scan()
//    scan.setTimeRange(timeRange._1, timeRange._2)
//    val proto = ProtobufUtil.toScan(scan)
//    val scanRange =Base64.encodeBytes(proto.toByteArray)
//
//    configuration.set(TableInputFormat.INPUT_TABLE, tableName)
//    configuration.set(TableInputFormat.SCAN, scanRange)
//
//    // 使用Hadoop api来创建一个RDD
//    val hBaseRDD = sc.newAPIHadoopRDD(configuration,
//      classOf[TableInputFormat],
//      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
//      classOf[org.apache.hadoop.hbase.client.Result])
//
//    hBaseRDD
//  }


  def getHBaseConf(sc: SparkContext, confDir: String, tableName: String, stopTimeStamp: Long) : RDD[(ImmutableBytesWritable, Result)] = {


    val scan = new Scan()
    scan.setTimeStamp(stopTimeStamp)
    val proto = ProtobufUtil.toScan(scan)
    val scanRange =Base64.encodeBytes(proto.toByteArray)

    val configFile = HBaseUtil.readConfigFile(confDir)
    val configuration = HBaseUtil.setHBaseConfigure(configFile)

    configuration.set(TableInputFormat.INPUT_TABLE, tableName)
    configuration.set(TableInputFormat.SCAN_ROW_STOP, scanRange)

    // 使用Hadoop api来创建一个RDD
    val hBaseRDD = sc.newAPIHadoopRDD(configuration,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    hBaseRDD
  }

  /**
    * 读取内容信息
    *
    * @param sc Spark程序入口
    * @param hbaseConf hBase信息
    * @return RDD[url，标题，正文]
    * @author wc
    * @note rowNum 18
    */
  def getRDD(sc: SparkContext, hbaseConf: Configuration): RDD[String] = {

    val tableName = "wk_detail"
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)

    val hbaseRdd = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat]
      , classOf[ImmutableBytesWritable], classOf[Result])

    val news = hbaseRdd.map(x => {

      val a = x._2.getValue(Bytes.toBytes("basic"), Bytes.toBytes("url"))
      val b = x._2.getValue(Bytes.toBytes("basic"), Bytes.toBytes("title"))
      val c = x._2.getValue(Bytes.toBytes("basic"), Bytes.toBytes("content"))
      val aFormat = judgeChaser(a)
      val bFormat = judgeChaser(b)
      val cFormat = judgeChaser(c)
      new String(a, aFormat) + "\n\t" + new String(b, bFormat) + "\n\t" + new String(c, cFormat)

    }).cache()

    news
  }
}
