//package com.kunyan.dxdataprocess
//
//import java.text.SimpleDateFormat
//
//import org.apache.spark.{SparkConf, SparkContext}
//import util.HBaseUtil
//
//import scala.collection.mutable.ArrayBuffer
//
///**
//  * Created by QQ on 7/25/16.
//  */
//object TelecomDataProcess {
//
//  def getDayTimeStamp(startDay: String): Long = {
//
//    val sdf = new SimpleDateFormat("yyyy-MM-dd")
//    val dayStamp = sdf.parse(startDay).getTime
//
//    dayStamp
//  }
//
//  /**
//    * 给定时间范围，根据时间窗口长度，获取若干组时间窗口
//    *
//    * @param startTime 起始时间戳
//    * @param endTime 结束时间戳
//    * @param timeRange 事件窗口
//    * @return Array[(Long, Long)]
//    * @note rowNum:11
//    */
//  def makeHourTimeWindows(startTime: Long, endTime: Long, timeRange: Int): Array[(Long, Long)] = {
//
//    var count = startTime
//    val dayWindows = ArrayBuffer[(Long, Long)]()
//
//    do {
//
//      // (start, start + timeRange - 1)
//      dayWindows.append((count, count + 60L * 60 * 1000 * timeRange - 1))
//      count += 60L * 60 * 1000
//
//    } while (count < endTime)
//
//    dayWindows.toArray
//  }
//
//  def judgeTimeWindow(time: Long, timeWindow: Array[(Long, Long)]): (Long, Long) = {
//
//    timeWindow.foreach(line => {
//      if (time >= line._1 && time <= line._2){
//        return line
//      }
//    })
//
//    (-1L, -1L)
//  }
//
//  def urlFormat(url: String): String = {
//
//    val temp = url.split("://")
//
//    temp.length match {
//      case 1 => temp(0).replaceAll("wwww", "")
//      case 2 => temp(1).replaceAll("wwww", "")
//    }
//  }
//
//  def main(args: Array[String]) {
//
//    val conf = new SparkConf()
//      .setAppName(s"Warren_TelecomData_Processing_${args(0)}")
//      .set("dfs.replication", "1")
//    //      .setMaster("local")
//    //      .set("spark.driver.host","192.168.2.90")
//    val sc = new SparkContext(conf)
//
//    val jsonConfig = new JsonConfig
//    jsonConfig.initConfig(args(1))
//
//    val hbaseConfig = HBaseUtil.getHbaseConf(jsonConfig.getValue("hbase", "rootDir"),
//      jsonConfig.getValue("hbase", "ips"))
//
//    val startDayTimeStamp = getDayTimeStamp(args(0))
//    val endDayTimeStamp = startDayTimeStamp + 24L * 60 * 60 * 1000
//
//    // 获取时间窗口
//    val timeRanges = sc.broadcast(makeHourTimeWindows(startDayTimeStamp, endDayTimeStamp, 1))
//
//    // 获取电信数据
//    val teleData = sc.textFile(jsonConfig.getValue("tp", "telecomDataPath") + s"/${args(0)}}",
//      jsonConfig.getValue("tp", "partition").toInt)
//
//    // 获取所有需要匹配的，并广播
//    val urlsBr = sc.broadcast(HBaseUtil.getRDD(sc, hbaseConfig).map(x => urlFormat(x.split("\n\t")(0))).collect()) // 这一步需要对从其他地方获取到新闻url做一些处理，例如去掉www和http
//
//    // 分组计算
//    teleData.map(row => {
//      val tmp = row.split("\t")
//      val url = urlFormat(tmp(3) + tmp(4))
//      val time = tmp(0)
//
//      (url, time)
//    }).filter(x => urlsBr.value.contains(x._1)).map(row => {
//
//      val timeWindow = judgeTimeWindow(row._2.toLong, timeRanges.value)
//
//      ((timeWindow._1, timeWindow._2, row._1), 1L)
//    }).reduceByKey(_ + _).saveAsTextFile(jsonConfig.getValue("tp", "outputPath") + s"/${args(0)}")
//  }
//}
