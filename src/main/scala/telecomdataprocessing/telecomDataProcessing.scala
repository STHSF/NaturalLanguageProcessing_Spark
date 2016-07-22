package telecomdataprocessing

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import util.{LoggerUtil, HBaseUtil}

import scala.collection.mutable

/**
  * Created by li on 16/7/18.
  * 匹配访问量和搜索量(由于每篇文本的访问量是随时间递增的,所以不可能全部统计,需要考虑新闻的时效性,
  * 初步确定统计日访问量)
  */
object TelecomDataProcessing {

  /**
    * 设置制定的时间范围(一天)
    *
    * @param setTime 指定的日期
    * @return 指定日期至前一天时间范围
    * @author
    */
  def setAssignedTimeRange(setTime: String): (Long, Long) = {

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

    val date = dateFormat.parse(setTime)
    val endTime = new Date(date.getTime - 24 * 60 * 60 * 1000)
    val stopTime = dateFormat.format(endTime)

    val startDate = setTime + "-00-00-00"
    val stopDate = stopTime  + "-00-00-00"

    val sdf = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
    val startRawDay = sdf.parse(startDate).getTime
    val stopRawDay = sdf.parse(stopDate).getTime

    (stopRawDay, startRawDay)
  }

  /**
    * 设置制定的时间范围(一天)内每小时时间间隔
    *
    * @param setTime 指定的日期
    * @return 指定日期至前一天时间范围每小时的时间间隔
    * @author
    */
  def setAssignedHourRange(setTime: String): Array[(Long, Long)] = {

    val date = setTime + "-00-00-00"

    val sdf = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")

    // 按小时变动
    val timeRangeIndex = new mutable.ArrayBuffer[(Long, Long)]

    for (i <- 0 until 24) {

      val startRawHour = sdf.parse(date).getTime + i * 60 * 60 * 1000
      val stopRawHour = sdf.parse(date).getTime  + (i + 1) * 60 * 60 * 1000
      val st = sdf.format(startRawHour)
      val sp = sdf.format(stopRawHour)
//      println(st, sp)

      timeRangeIndex.+=((startRawHour, stopRawHour))

    }

    timeRangeIndex.toArray
  }

  /**
    * 电信数据获取
    *
    * @param sc SparkContext
    * @param dir 数据存储的位置
    * @param setTime 读取数据的时间
    * @return
    */
  def dataReadFromHDFS(sc:SparkContext, dir: String, setTime: String): RDD[(String, String, String)] = {

    val filePath = dir + setTime + ".tar.gz"
    val data = sc.textFile(filePath)

    val url =  data.map(_.split("\t")).filter(_.length == 8).map(x => (x(0), x(3) + x(4), x(5)))

    url
  }

  /**
    * 新闻文本读取
    * 获取hbase中的内容
    *
    * @param sc SparkContext
    * @param confDir 配置文件所在的文件夹
    */
  def getHBaseConf(sc: SparkContext, confDir: String, timeRange: (Long, Long), tableName: String) : RDD[(ImmutableBytesWritable, Result)] = {

    val configFile = HBaseUtil.readConfigFile(confDir)
    val configuration = HBaseUtil.setHBaseConfigure(configFile)


    // 读取hbase中的文件
//    val tableName = "wk_detail"  // 表名

    val scan = new Scan()
    scan.setTimeRange(timeRange._1, timeRange._2)
    val proto = ProtobufUtil.toScan(scan)
    val scanRange =Base64.encodeBytes(proto.toByteArray)

    configuration.set(TableInputFormat.INPUT_TABLE, tableName)
    configuration.set(TableInputFormat.SCAN, scanRange)

    // 使用Hadoop api来创建一个RDD
    val hBaseRDD = sc.newAPIHadoopRDD(configuration,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    hBaseRDD
  }

  /**
    * 新闻文本读取
    * 获取hbase中的内容
    *
    * @param hBaseRDD
    * @return
    */
  def newsReadFromHBase(hBaseRDD: RDD[(ImmutableBytesWritable, Result)]): RDD[String] = {

    val news = hBaseRDD.map { x => {

      val url = x._2.getValue(Bytes.toBytes("basic"), Bytes.toBytes("url"))

      val urlFormat = HBaseUtil.judgeChaser(url)

      new String(url, urlFormat)
    }}.cache()

    news
  }

  /**
    * 匹配
    *
    * @param telecomData
    * @param newsData
    * @return
    */
  def urlMatching(telecomData: RDD[String], newsData: RDD[String]): Array[(String, Long)] = {


    val result = new mutable.ArrayBuffer[(String, Long)]
    newsData.map { url =>

      val num = telecomData.filter(_.contains(url)).count()
      result.+=((url, num))
    }

    result.toArray
  }

  /**
    * 测试
    *
    * @param args
    */
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("telecomdataprocessing")
    val sc = new SparkContext(conf)

    val setTime = args(0)

    //设置时间段,一小时为一个间隔
    val timeRangeHour = setAssignedHourRange(setTime)

    // Hdfs上的数据,一天的数据
    val dir = args(1)
    val dataFromHDFS = dataReadFromHDFS(sc, dir, setTime)

    // hbase上的数据
    val confDir = args(2) // hbase配置文件目录
    val tableName = args(3)  // 表名

    val result = new mutable.ArrayBuffer[(String, Array[(String, Long)])]

    for (item <- 0 until 24) {

      val temp = dataFromHDFS.filter { line => {

        timeRangeHour(item)._1 <= line._1.toLong && line._1.toLong <= timeRangeHour(item)._2
      }}.map (x => x._2)

      val hBaseConf = getHBaseConf(sc, confDir, timeRangeHour(item), tableName)

      val newsFromHBase = newsReadFromHBase(hBaseConf)

      val res = urlMatching(temp, newsFromHBase)

      result.+=((item.toString, res))

    }

    val resultPath = args(4)

    val writer = new PrintWriter(new File(resultPath +"%s".format(result) + ".txt"))

    result.foreach {
      line => {
        writer.write("%s\t%s\n".format(line._1, line._2.mkString(",")))
      }
    }

    LoggerUtil.warn("词向量写入结束 》》》》》》》》》》》》")
    writer.close()

    sc.stop()

  }

}
