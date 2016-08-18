//import java.text.SimpleDateFormat
//import java.util.Date
//
//import org.apache.hadoop.hbase.client.Result
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable
//import org.apache.hadoop.hbase.util.Bytes
//import org.apache.spark.rdd.RDD
//import org.apache.spark.{SparkConf, SparkContext}
//import util.{HBaseUtil, LoggerUtil, TimeUtil}
//
//import scala.collection.mutable
//
///**
//  * Created by li on 16/7/20.
//  */
//object TelecomDataProcessing {
//
//  /**
//    * 设置指定的时间范围
//    *
//    * @param setTime 指定的日期
//    * @return 指定日期至当前时间的前一天时间范围
//    * @author Li Yu
//    * @note rowNum: 10
//    */
//  def setAssignedTimeRange(setTime: String): (Long, Long) = {
//
//    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
//
//    //获取当前时间
//    val endTime = TimeUtil.getDay
//    val date = dateFormat.parse(endTime)
//    //当前时间前一天时间
//    val stopTime = new Date(date.getTime - 24 * 60 * 60 * 1000)
//    val stop = dateFormat.format(stopTime)
//
//    val startDate = setTime + "-00-00-00"
//    val stopDate = stop  + "-00-00-00"
//
//    val sdf = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
//    val startRawDay = sdf.parse(startDate).getTime
//    val stopRawDay = sdf.parse(stopDate).getTime
//
//    (startRawDay, stopRawDay)
//  }
//
//  /**
//    * 设置制定的时间范围每小时时间间隔
//    *
//    * @param setTime 指定的日期
//    * @return 指定日期至前一天时间(24小时)范围每小时时间戳间隔
//    * @author Li Yu
//    * @note rowNum: 11
//    */
//  def setAssignedHourRange(setTime: String): Array[(Long, Long)] = {
//
//    val date = setTime + "-00-00-00"
//
//    val sdf = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
//
//    // 按小时变动
//    val timeRangeIndex = new mutable.ArrayBuffer[(Long, Long)]
//
//    for (i <- 0 until 24) {
//
//      val startRawHour = sdf.parse(date).getTime + i * 60 * 60 * 1000
//      val stopRawHour = sdf.parse(date).getTime  + (i + 1) * 60 * 60 * 1000
//
//      timeRangeIndex.+=((startRawHour, stopRawHour))
//    }
//
//    timeRangeIndex.toArray
//  }
//
//  /**
//    * 电信数据获取
//    *
//    * @param sc SparkContext
//    * @param dir 数据存储的位置
//    * @param setTime 读取数据的时间
//    * @return Li Yu
//    * @note rowNum: 5
//    */
//  def dataReadFromHDFS(sc:SparkContext, dir: String, setTime: String): RDD[(String, String, String)] = {
//
//    val filePath = dir + setTime + ".tar.gz"
//    val data = sc.textFile(filePath, 10)
//
//    val url =  data.map(_.split("\t")).filter(_.length == 8).map(x => (x(0), x(3) + x(4), x(5)))
//
//    url
//  }
//
//  /**
//    * 新闻文本读取
//    * 获取hbase中的内容
//    *
//    * @param hBaseRDD 获取hbase中的内容
//    * @return
//    * @author LiYu
//    * @note rowNum: 6
//    */
//  def newsReadFromHBase(hBaseRDD: RDD[(ImmutableBytesWritable, Result)]): RDD[String] = {
//
//    val news = hBaseRDD.map { x => {
//
//      val url = x._2.getValue(Bytes.toBytes("basic"), Bytes.toBytes("url"))
//
//      val urlFormat = HBaseUtil.judgeChaser(url)
//
//      new String(url, urlFormat)
//
//    }}
//
//    news
//  }
//
//  /**
//    * url筛选,将url中的https://|| www.|| http://www.||去除.
//    *
//    * @param url 待筛选的url
//    * @return 筛选过的url
//    * @author LiYu
//    * @note rowNum: 7
//    */
//  def urlFilter(url: RDD[String]): RDD[String] = {
//
//    val result = url.map(x => x.split("://")).map { x =>
//
//      if(x.length == 1) {
//
//        x(0).replace("www.", "")
//      } else {
//
//        x(1).replace("www.", "")
//      }
//    }
//
//    result
//  }
//
//  /**
//    * 匹配电信url和新闻url
//    *
//    * @param telecomData 电信数据
//    * @param news 新闻数据
//    * @return
//    * @author LiYu
//    * @note rowNum: 3
//    */
//  def urlMatching(telecomData: RDD[String], news: RDD[String]): Array[(String, Long)] = {
//
//    val newsData = news.collect()
//
//    val result = telecomData.filter(x => newsData.contains(x)).map((_, 1L)).reduceByKey(_ + _)
//
//    result.collect()
//  }
//
//  /**
//    * 主程序
//    *
//    * @param args 参数
//    * @author LiYu
//    * @note rowNum: 26
//    */
//  def main(args: Array[String]) {
//
//    val conf = new SparkConf().setAppName("Warren_TelecomData_Processing")
//    val sc = new SparkContext(conf)
//
//    //设置需要计算的某一天(setTime)的某一时间段(timeRangeST--timeRangeED)内的新闻.
//    val setTime = args(0)
//    val timeRangeST = args(1).toInt  // 开始时间
//    val timeRangeED = args(2).toInt  // 结束时间
//
//    //设置时间段,一小时为一个间隔
//    val timeRangeHour = setAssignedHourRange(setTime)
//
//    // HDFS的数据目录
//    val dir = args(3)
//
//    // HBase的数据
//    val confDir = args(4)  // HBase配置文件目录
//    val tableName = args(5)  // 表名
//
//    //结果保存的变量
//    val result = new mutable.ArrayBuffer[(String)]
//
//    //循环遍历从开始时间段到结束时间段中HBase中的新闻数据
//    for (item <- timeRangeST until timeRangeED) {
//
//      // 转换数据格式,将item变成时间的格式,用于标识结果输出
//      val dataFormat = new SimpleDateFormat("yyyy-MM-dd")
//      val seTime = dataFormat.parse(setTime)
//      val dateTime = new Date(seTime.getTime + item * 60 * 60 * 1000)
//      val sdf = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
//      val newsTimeLine = sdf.format(dateTime)
//
//      //从HBase中获取新闻数据
//      LoggerUtil.warn("从hBase数据读 " + "%s".format(newsTimeLine) + " 时刻的数据"+" 》》》》》》》》》》》》")
//      val hBaseConf = HBaseUtil.getHBaseConf(sc, confDir, timeRangeHour(item), tableName)
//      val newsFromHBase = newsReadFromHBase(hBaseConf)
//      LoggerUtil.warn("hBase" + "%s".format(newsTimeLine) + " 时刻的数据读取结束 》》》》》》》》》》》》")
//
//      //url过滤
//      val newsFiltered = TelecomDataProcessing.urlFilter(newsFromHBase)
//
//      // 设置从读取hdfs上的数据的时间
//      val hourRange = setAssignedTimeRange(setTime)
//      val startTime = hourRange._1
//      val stopTime = hourRange._2
//      val day = (stopTime - startTime) / (24 * 60 * 60 * 1000)
//      LoggerUtil.warn("共需从hdfs数据读 " + "%s".format(day) + " 天的数据"+" 》》》》》》》》》》》》")
//
//      // 取出每一天的数据
//      for (i <- 0 to day.toInt ) {
//
//        // 转换数据格式,将i转换成时间的格式,用于标识输出结果.
//        val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
//        val date = dateFormat.parse(setTime)
//        val stopTime = new Date(date.getTime + i * 24 * 60 * 60 * 1000)
//        val dayTime = dateFormat.format(stopTime)
//
//        LoggerUtil.warn("从hdfs数据读 " + "%s".format(dayTime) + " 日的数据"+" 》》》》》》》》》》》》")
//        val dataFromHDFS = dataReadFromHDFS(sc, dir, dayTime)
//          .filter(! _._1.contains("home/telecom"))
//        LoggerUtil.warn("hdfs一天的数据读取结束 》》》》》》》》》》》》")
//
//        // 对每一天中每一小时的数据进行统计
//        for (j <- 0 until 24) {
//
//          //获取每个小时的电信数据
//          LoggerUtil.warn("从hdfs数据读 " + "%s".format(dayTime) + " 日" + "%s".format(j) + " 小时的数据" + " 》》》》》》")
//          val telecomData = dataFromHDFS.filter { line => {
//
//            timeRangeHour(j)._1 <= line._1.toLong && line._1.toLong <= timeRangeHour(j)._2
//
//          }}.map (x => x._2)
//          LoggerUtil.warn("hdfs一小时数据读取结束 》》》》》》》》》》》》")
//
//          //url过滤
//          val telecomDataFiltered = TelecomDataProcessing.urlFilter(telecomData)
//          LoggerUtil.warn("hdfs中url过滤结束 》》》》》》》》》》》》")
//
//          //url匹配
//          LoggerUtil.warn("hbase和hdfs中的url匹配开始 》》》》》》》》》》》》")
//          val res = TelecomDataProcessing.urlMatching(telecomDataFiltered, newsFiltered)
//            .map(x => x._1 + "," + x._2)
//            .mkString("\t")
//          LoggerUtil.warn("%s".format(dayTime) + " 日" + "%s".format(j) + " 时的url匹配结束" + " 》》》》》》")
//
//          result.+=(newsTimeLine + "|"+ dayTime + "||" + res + "\n")
//        }
//      }
//    }
//
//    // 匹配结果保存
//    val modelPath = args(6)
//    val time = TimeUtil.getCurrentHour
//
//    sc.parallelize(result.toSeq).saveAsTextFile(modelPath + "%s".format(time) + "-urlMatchResult")
//
//    LoggerUtil.warn("电信数据url匹配程序结束 》》》》》》》》》》》》")
//
//    sc.stop()
//  }
//
//}
