package dataprocess.vipstockstatistic

import java.text.SimpleDateFormat

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import util.{HBaseUtil, LoggerUtil, RedisUtil}

/**
  * Created by li on 16/8/23.
  * 统计大V新闻,分析文章中涉及到的股票的情感趋势.
  */
object VipStockStatistic {

  /**
    * 获取输入时间的时间戳范围,以天为单位
    *
    * @param startDay 输入时间
    * @return
    * @return Li Yu
    * @note rowNum: 4
    */
  def getDayTimeStampTimeRange(startDay: String): (Long, Long) = {

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val dayStampStart = sdf.parse(startDay).getTime
    val dayStampStop = dayStampStart + 24 * 60 * 60 * 1000 - 1

    (dayStampStart, dayStampStop)
  }

  /**
    * 获取hBase中的新闻文本内容
    *
    * @param hBaseRDD 获取hBase中的内容
    * @return
    * @author Li Yu
    * @note rowNum: 6
    */
  def newsReadFromHBase(hBaseRDD: RDD[(ImmutableBytesWritable, Result)]): RDD[String] = {

    val news = hBaseRDD.map{row => {

      val url = row._2.getRow
      val timeStamp = Bytes.toLong(row._2.getValue(Bytes.toBytes("basic"), Bytes.toBytes("time")))
      val content = row._2.getValue(Bytes.toBytes("basic"), Bytes.toBytes("content"))
      val platform = row._2.getValue(Bytes.toBytes("basic"), Bytes.toBytes("platform"))

      val urlFormat = HBaseUtil.judgeChaser(url)
      val contentFormat = HBaseUtil.judgeChaser(content)
      val platformFormat = HBaseUtil.judgeChaser(platform)

      new String(platform, platformFormat) + "\t" + timeStamp +
        "\t" + new String(url, urlFormat) + "\t" + new String(content, contentFormat)
    }}

    news
  }

  /**
    * 统计大V新闻,分析文章中涉及到的股票的情感趋势.
    *
    * @param args 输入参数
    */
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Warren_VipStockStatistic")
    val sc = new SparkContext(conf)

    // 设置需要计算的某一天(setTime)的新闻.
    val setTime = args(0)
    // 配置文件目录
    val confDir = args(1)
    // hBase表名
    val tableName = args(2)
    // 股票词典目录
    val stockDicPath = args(3)
    // 情感词典目录
    val segDicPath = args(4)

    // 从HBase中获取新闻数据
    val hBaseConf = HBaseUtil.getHBaseConf(sc, confDir, tableName)
    val newsFromHBase = newsReadFromHBase(hBaseConf)

    // 获取读取HBase数据的截止时间
    val (startTimeStamp, stopTimeStamp) = getDayTimeStampTimeRange(setTime)
    LoggerUtil.warn(s"时间跨度${(startTimeStamp, stopTimeStamp)}")

    // 过滤hBase中的新闻数据,按平台号和时间间隔过滤
    val newsFromHB = newsFromHBase.map(row => row.split("\t"))
      .filter(x => (x(0).toLong >= 40000) && (x(0).toLong <= 50000))
      .filter(x => (x(1).toLong >= startTimeStamp) && (x(1).toLong <= stopTimeStamp))
      .cache()
    LoggerUtil.warn(s"vip新闻的数量${newsFromHB.count()}")

    //股票股票情感趋势分析[(vipID: url, segmentation)]
    val segDic = Array(segDicPath + "/user_dic.txt", segDicPath + "/pos_dic.txt",
      segDicPath + "/neg_dic.txt", segDicPath + "/fou_dic.txt")

    // 情感词典导入
    val dicMap = PredictWithDic.init(sc, segDic)

    // 新闻情感计算
    val newsSentiment = newsFromHB.map(x => (PredictWithDic.predict(x(3), dicMap), x(2))).groupByKey().cache()

    // 看涨新闻
    val newsRise = newsSentiment.filter(x => x._1.toInt == 1).flatMap(_._2).map((_, 1))
    LoggerUtil.warn(s"看涨新闻的数量${newsRise.count()}")

    // 看跌新闻
    val newsDown = newsSentiment.filter(x => x._1.toInt == -1).flatMap(_._2).map((_, 1))
    LoggerUtil.warn(s"看跌新闻的数量${newsDown.count()}")

    // 读取股票代码(3000只股票)
    val stockDic = sc.textFile(stockDicPath)
      .map(row => row.split("\t"))
      .map(row => (row(0), row(1).split(","))).collect().toMap

    // 调用分词程序,kunyan分词
    val corpusRes = CorpusBuild.run(confDir, newsFromHB)

    // 提取文章中的股票值((vipID: url, Array[stocksId]))
    val vipIdStocksId = corpusRes.map(x => (x._1, Regular.grep(x._2, stockDic)))
      .filter(_._2 != "") // 剔除匹配不到的股票代码的url
      .map(row => (row._1, row._2.split(",")))
      .cache()

    // 看涨股票号统计
    val resRise = vipIdStocksId.join(newsRise)
      .map(row => (row._1, row._2._1))
      .flatMapValues(x => x)
      .map(row => (row._2, row._1))
      .groupByKey()
      .map(row => (row._1, row._2.size.toString)).collect()
    LoggerUtil.warn(s"看涨股票的数量${resRise.length}")

    // 看跌股票号统计
    val resDown  = vipIdStocksId.join(newsDown)
      .map(row => (row._1, row._2._1))
      .flatMapValues(x => x)
      .map(row => (row._2, row._1))
      .groupByKey()
      .map(row => (row._1, row._2.size.toString)).collect()
    LoggerUtil.warn(s"看跌股票的数量${resDown.length}")

    // 未涉及股票统计
    val resNon = stockDic.keys.filterNot(x => resRise.map(_._1).contains(x) && resDown.map(_._1).contains(x))
      .map(row => (row, "0"))
      .toBuffer

    val stockRise = resNon.++(resRise).toArray
    val stockDown = resNon.++(resDown).toArray

    // 数据保存至redis
    RedisUtil.write2Redis(stockRise, setTime, "rise", confDir)
    RedisUtil.write2Redis(stockDown, setTime, "down", confDir)
    LoggerUtil.warn("统计数据保存结束==============")

    sc.stop()
  }

}
