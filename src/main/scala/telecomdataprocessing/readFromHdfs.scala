package telecomdataprocessing

import org.apache.spark.{SparkConf, SparkContext}
import util.LoggerUtil

/**
  * Created by li on 16/7/27.
  */
object readFromHdfs {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Warren_ReadFrom_Hdfs_filter")

    val sc = new SparkContext(conf)

    val hdfsDir = args(0)
//    val hdfsDir = "hdfs://222.73.57.12:9000/telecom/shdx/origin/data/"

    val setTime = args(1)
//    val setTime = "2016-07-23"


    val time = System.currentTimeMillis()

    LoggerUtil.warn("time2Start:" +"%s".format(time)+ " 》》》》》》》》》》》》")
    // 数据获取开始和截止时间
    val stopTimeStamp = TDP.getDayTimeStamp(setTime)
    val startTimeStamp = stopTimeStamp - 24 * 60 * 60 * 1000
    val timeRanges = sc.broadcast(TDP.makeHourTimeWindows(startTimeStamp, stopTimeStamp -1, 1))

    // 23个新闻网站的host域名
    val urlUnion = Array("yicai.com", "21cn.com", "d.weibo.com","xueqiu.com","10jqka.com.cn","gw.com.cn",
    "eastmoney.com","p5w.net","stockstar.com","hexun.com","caijing.com.cn","jrj.com.cn","cfi.net.cn","cs.com.cn",
    "cnstock.com", "stcn.com","news.cn","finance.ifeng.com","finance.sina.com.cn","business.sohu.com","money.163.com",
      "wallstreetcn.com","finance.qq.com","moer.jiemian.com","www.szse.cn","weixin.sogou.com","sse.com.cn","zqyjbg.com")

    val dataFromHDFS2 = sc.textFile(hdfsDir + setTime + "/*")
      .filter(! _.contains("home/telecom"))
      .filter(! _.contains("youchaojiang"))
      .map(_.split("\t"))
      .filter(_.length == 8)
      .filter(x => urlUnion.contains(TDP.urlFormat(x(3))))
      .map(x => (TDP.urlFormat(x(3) + x(4)), x(0)))

    val result = dataFromHDFS2.map(row => {

      val timeWindow = TDP.judgeTimeWindow(row._2.toLong, timeRanges.value)

      ((timeWindow._1, timeWindow._2, row._1), 1L)
    }).reduceByKey(_ + _).count()

    println(result)


    LoggerUtil.warn("time2End:" +"%s".format(time)+ " 》》》》》》》》》》》》")

  }

}
