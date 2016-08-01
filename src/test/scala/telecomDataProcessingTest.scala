//import org.apache.spark.{SparkConf, SparkContext}
//
//import scala.collection.mutable
//
///**
//  * Created by li on 16/7/20.
//  */
//object TelecomDataProcessingTest {
//
//
//  def main(args: Array[String]) {
//
//    val conf = new SparkConf().setAppName("test").setMaster("local")
//    val sc = new SparkContext(conf)
//
//    val setTime = "2016-07-15"
//
//    //设置时间段,一小时为一个间隔
//    val timeRangeHour = TelecomDataProcessing.setAssignedHourRange(setTime)
//
//    // Hdfs上的数据,一天的数据
//    val dir = "hdfs://222.73.57.12:9000/telecom/shdx/origin/data/"
//    val dataFromHDFS = TelecomDataProcessing.dataReadFromHDFS(sc, dir, setTime).filter(! _._1.contains("home/telecom"))
//
//    println("dataFromHDFS结束")
//    // dataFromHDFS.foreach(println)
//
//    // hbase上的数据
//    val confDir = "/Users/li/kunyan/NaturalLanguageProcessing/src/main/scala/util/config.xml" // hbase配置文件目录
//    val tableName = "wk_detail" // 表名
//
//    val result = new mutable.ArrayBuffer[(String, Array[(String, Long)])]
//
//    for (item <- 0 until 1) {
//
//      val temp = dataFromHDFS.filter { line => {
//
//        (timeRangeHour(item)._1 <= line._1.toLong) && (line._1.toLong <= timeRangeHour(item)._2)
//
//      }}.map(_._2)
//
//      println("temp读取结束")
//
//      temp.foreach(println)
//
//      val hBaseConf = TelecomDataProcessing.getHBaseConf(sc, confDir, timeRangeHour(item), tableName)
//
//      val newsFromHBase = TelecomDataProcessing.newsReadFromHBase(hBaseConf)
//
//      newsFromHBase.foreach(println)
//
//      val res = TelecomDataProcessing.urlMatching(temp, newsFromHBase)
//
//      result.+=((item.toString, res))
//
//    }
//
//    result.toArray.foreach( x => {
//      println(x._1)
//      x._2.foreach(x =>  println((x._1, x._2)))
//    })
//
//
//    sc.stop()
//
//  }
//
//}
