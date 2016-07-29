package telecomdataprocessing

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import util.{HBaseUtil, LoggerUtil}

/**
  * Created by li on 16/7/26.
  */
object TDP {

  /**
    *
    * @param startDay
    * @return
    */
  def getDayTimeStamp(startDay: String): Long = {

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val dayStamp = sdf.parse(startDay).getTime

    dayStamp
  }

  /**
    * 获取给定时间戳所对应的当日00：00：00的时间戳
    *
    * @param timeStamp 时间戳
    * @return 当日时间戳
    * @note rowNum:6
    */
  def getDayTimeStamp(timeStamp: Long): Long = {

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val dayString = sdf.format(timeStamp)
    val dayStamp = sdf.parse(dayString).getTime

    dayStamp
  }

  def getCurrentDayTimeStamp: Long = {

    getDayTimeStamp(new Date().getTime)
  }

  /**
    *
    * @param url
    * @return
    */
  def urlFormat(url: String): String = {

    val x = url.split("://")

    if(x.length == 1) {

      x(0).replace("www.", "")
    } else {

      x(1).replace("www.", "")
    }
  }

  /**
    * 电信数据获取
    *
    * @param sc SparkContext
    * @param dir 数据存储的位置
    * @param setTime 读取数据的时间
    * @return Li Yu
    * @note rowNum: 5
    */
  def dataReadFromHDFS(sc:SparkContext, dir: String, setTime: String): RDD[(String, String)] = {

    val filePath = dir + setTime + "/*"
    val data = sc.textFile(filePath, 10)
      .filter(x => ! x.contains("home/telecom") || ! x.contains("youchaojiang") || ! x.contains("ustar"))

    val result =  data.map(_.split("\t"))
      .filter(_.length == 8)
      .map(x => (x(3) + x(4), x(0)))

    result
  }

  /**
    * 新闻文本读取
    * 获取hbase中的内容
    *
    * @param hBaseRDD 获取hbase中的内容
    * @return
    * @author LiYu
    * @note rowNum: 6
    */
  def newsReadFromHBase(hBaseRDD: RDD[(ImmutableBytesWritable, Result)]): RDD[String] = {

    val news = hBaseRDD.map { x => {

      val url = x._2.getValue(Bytes.toBytes("basic"), Bytes.toBytes("url"))

      val timeStamp = x._2.getColumnLatestCell(Bytes.toBytes("basic"), Bytes.toBytes("url")).getTimestamp

      val urlFormat = HBaseUtil.judgeChaser(url)

      // timeStamp + "\t" + new String(url, urlFormat)
      new String(url, urlFormat)

    }}

    news
  }

  /**
    * url筛选,将url中的https://|| www.|| http://www.||去除.
    *
    * @param url 待筛选的url
    * @return 筛选过的url
    * @author LiYu
    * @note rowNum: 7
    */
  def urlFilter(url: RDD[String]): RDD[String] = {

    val result = url.map(x => x.split("://"))
      .map { x =>

      if(x.length == 1) {

        x(0).replace("www.", "")
      } else {

        x(1).replace("www.", "")
      }
    }

    result
  }


  /**
    * 匹配电信url和新闻url
    *
    * @param telecomData 电信数据
    * @param news 新闻数据
    * @return
    * @author QiuQiu LiYu
    * @note rowNum: 3
    */
  def urlMatch(telecomData: RDD[(String, String)],
               news: RDD[String]): RDD[((String, String), Long)] = {

    val newsTemp = news.map(x => (x, "1"))

    val result = newsTemp.join(telecomData)
      .map(x => (x._1, x._2._2))
      .map(row => {

      ((row._2, row._1), 1L)
    }).reduceByKey(_ + _)

    result
  }

  /**
    * 主程序
    *
    * @param args 参数
    * @author LiYu
    * @note rowNum: 26
    */
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Warren_TelecomData_Processing" + "%s".format(args(0)))
    val sc = new SparkContext(conf)

    // 设置需要计算的某一天(setTime)的新闻.
    val setTime = args(0)

    // HDFS的数据目录
    val hdfsDir = args(1)

    // HBase的数据
    val confDir = args(2)  // HBase配置文件目录
    val tableName = args(3)  // 表名

    // 数据获取开始和截止时间
    val stopTimeStamp = getDayTimeStamp(setTime)
    val startTimeStamp = stopTimeStamp - 24 * 60 * 60 * 1000

    // 从HBase中获取新闻数据,获取截止时间之前的数据
    LoggerUtil.warn("从hBase数据读 " + " 时刻的数据"+" 》》》》》》》》》》》》")
    val hBaseConf = HBaseUtil.getHBaseConf(sc, confDir, tableName, getDayTimeStamp(setTime))
    val newsFromHBase = newsReadFromHBase(hBaseConf)
    LoggerUtil.warn("hBase" + " 时刻的数据读取结束 》》》》》》》》》》》》")

    // hbase中的url过滤
    val newsFiltered = urlFilter(newsFromHBase)

    // 转换时间数据的格式
    val sdf = new SimpleDateFormat("yyyy-MM-dd-HH")

    // 23个新闻网站的host
    val urlUnion = Array("yicai.com", "21cn.com", "d.weibo.com", "xueqiu.com", "10jqka.com.cn", "gw.com.cn",
      "eastmoney.com", "p5w.net", "stockstar.com", "hexun.com", "caijing.com.cn", "jrj.com.cn", "cfi.net.cn", "cs.com.cn",
      "cnstock.com", "stcn.com", "news.cn", "finance.ifeng.com", "finance.sina.com.cn", "business.sohu.com", "money.163.com",
      "wallstreetcn.com", "finance.qq.com", "moer.jiemian.com", "www.szse.cn", "weixin.sogou.com", "sse.com.cn", "zqyjbg.com")

    LoggerUtil.warn("读取hdfs上的数据 》》》》》》》》》》》》")
    // 读取hdfs上的数据
    val dataFromHdfs = sc.textFile(hdfsDir + setTime + "/*")
      .filter(! _.contains("home/telecom"))
      .filter(! _.contains("youchaojiang"))
      .filter(! _.contains("ustar"))
      .map(_.split("\t"))
      .filter(_.length == 8)
      .filter(x => urlUnion.contains(urlFormat(x(3))))
      .map(x => (urlFormat(x(3) + x(4)), sdf.format(x(0).toLong)))

    LoggerUtil.warn("hdfs上的数据读取结束 》》》》》》》》》》》》")

    val result = urlMatch(dataFromHdfs, newsFiltered)
        .map(x => "(" + x._1._1 + "," + x._1._2 +")" + "\t" + x._2+ ")" + "\n")
    LoggerUtil.warn("匹配结束 》》》》》》》》》》》》")

    // 匹配结果保存
    val modelPath = args(4)
    val time = new Date().getTime

    result.saveAsTextFile(modelPath + "%s".format(time) + "-urlMatchResult")

    LoggerUtil.warn("电信数据url匹配程序结束 》》》》》》》》》》》》")

    sc.stop()
  }

}
