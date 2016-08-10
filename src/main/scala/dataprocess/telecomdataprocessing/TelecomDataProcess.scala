package telecomdataprocessing

import java.text.SimpleDateFormat

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import util.{HBaseUtil, LoggerUtil}

/**
  * Created by li on 16/7/26.
  */
object TelecomDataProcess {

  /**
    * 获取输入时间的时间戳
    *
    * @param startDay 输入时间
    * @return
    * @return Qiu Qiu
    * @note rowNum: 4
    */
  def getDayTimeStamp(startDay: String): Long = {

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val dayStamp = sdf.parse(startDay).getTime

    dayStamp
  }

  /**
    * 单个url筛选,将url中的https://|| www.|| http://www.||去除.
    *
    * @param url 需要格式化的url
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
      .filter(_.length >= 7)
      .map(x => (x(3) + x(4), x(0)))

    result
  }

  /**
    * 新闻文本读取
    * 获取hbase中的内容
    *
    * @param hBaseRDD 获取hbase中的内容
    * @return
    * @author Li Yu
    * @note rowNum: 6
    */
  def newsReadFromHBase(hBaseRDD: RDD[(ImmutableBytesWritable, Result)]): RDD[String] = {

    val news = hBaseRDD.map { x => {

      val url = x._2.getRow

      val timeStamp = Bytes.toLong(x._2.getValue(Bytes.toBytes("basic"), Bytes.toBytes("time")))

      val urlFormat = HBaseUtil.judgeChaser(url)

      timeStamp + "\t" + new String(url, urlFormat)

    }}

    news
  }

  /**
    * url筛选,将url中的https://|| www.|| http://www.||去除.
    *
    * @param url 待筛选的url
    * @return 筛选过的url 和原始的url
    * @author LiYu
    * @note rowNum: 7
    */
  def urlFilter(url: RDD[String]): RDD[(String, String)] = {

    val result = url.map(x => (x.split("://"), x))
      .map { x =>

      if(x._1.length == 1) {

        (x._1(0).replace("www.", ""), x._2)
      } else {

        (x._1(1).replace("www.", ""), x._2)
      }
    }

    result
  }

  /**
    * 匹配电信url和新闻url
    *
    * @param telecomData 电信数据
    * @param newsTemp 新闻数据
    * @return
    * @author Li Yu, Qiu Qiu, Wang Cao
    * @note rowNum: 8
    */
  def urlMatch(telecomData: RDD[(String, String)],
               newsTemp: RDD[(String, String)]): RDD[((String, String), Long)] = {

    val result = newsTemp.join(telecomData)
      .map(x => (x._2._1, x._2._2))
      .map(row => {

      ((row._2, row._1), 1L)
    }).reduceByKey(_ + _)

    result
  }

  /**
    * 主程序
    *
    * @param args 参数
    * @author Li Yu
    * @note rowNum: 26
    */
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Warren_TelecomData_Processing" + "%s".format(args(0)))
    val sc = new SparkContext(conf)

    // 设置需要计算的某一天(setTime)的新闻.
    val timeStyle = args(0)

    // HDFS的数据目录
    val hdfsDir = args(1)

    // HBase的数据
    val confDir = args(2)  // HBase配置文件目录
    val tableName = args(3)  // 表名

    // 从HBase中获取新闻数据
    LoggerUtil.warn("从hBase数据读 " + " 时刻的数据"+" 》》》》》》》》》》》》")
    val hBaseConf = HBaseUtil.getHBaseConf(sc, confDir, tableName)
    val newsFromHBase = newsReadFromHBase(hBaseConf)
    LoggerUtil.warn("hBase" + " 时刻的数据读取结束 》》》》》》》》》》》》")

    // 获取读取HBase数据的截止时间
    val stopTimeStamp = getDayTimeStamp(timeStyle)

    val newsFromHB = newsFromHBase.map(row => row.split("\t")).filter(x => x(0).toLong < stopTimeStamp).map(_(1))

    // hbase中的url过滤
    val newsFiltered = urlFilter(newsFromHB)

    // 转换时间数据的格式,
    val dataFormat = new SimpleDateFormat("yyyy-MM-dd-HH")

    // 23个新闻网站的host集合
    val urlUnion = Array("yicai.com", "21cn.com", "d.weibo.com", "xueqiu.com", "10jqka.com.cn", "gw.com.cn",
      "eastmoney.com", "p5w.net", "stockstar.com", "hexun.com", "caijing.com.cn", "jrj.com.cn", "cfi.net.cn", "cs.com.cn",
      "cnstock.com", "stcn.com", "news.cn", "finance.ifeng.com", "finance.sina.com.cn", "business.sohu.com", "money.163.com",
      "wallstreetcn.com", "finance.qq.com", "moer.jiemian.com", "www.szse.cn", "weixin.sogou.com", "sse.com.cn", "zqyjbg.com")

    LoggerUtil.warn("开始读取hdfs上的数据 》》》》》》》》》》》》")
    val dataFromHdfs = sc.textFile(hdfsDir + timeStyle)
      .filter(! _.contains("home/telecom"))  //过滤脏数据
      .filter(! _.contains("youchaojiang"))
      .filter(! _.contains("ustar"))
      .map(_.split("\t"))
      .filter(_.length >= 7)  // 有些数据最后数据本身包含换行导致数据字段缺失的情况
      .filter(x => urlUnion.contains(urlFormat(x(3))))  // 先通过新闻网站的host过滤无用的超大量数据
      .map(x => (urlFormat(x(3) + x(4)), dataFormat.format(x(0).toLong))) // 利用时间戳转化的时间时段标示电信url

    LoggerUtil.warn("hdfs上的数据读取结束 》》》》》》》》》》》》")

    val result = urlMatch(dataFromHdfs, newsFiltered)
        .map(x =>  x._1._1 + "," + x._1._2 + "\t" + x._2 + "\n")
    LoggerUtil.warn("匹配结束 》》》》》》》》》》》》")

    // 匹配结果保存
    val modelPath = args(4)

    LoggerUtil.warn("数据保存 》》》》》》》》》》》》")
    result.saveAsTextFile(modelPath + "%s".format(timeStyle) + "-PVResult")

    LoggerUtil.warn("新闻访问量统计程序结束 》》》》》》》》》》》》")

    sc.stop()
  }

}
