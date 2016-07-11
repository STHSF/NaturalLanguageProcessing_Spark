package meachinelearning.hotdegreecalculate

import java.io.{File, PrintWriter}

import util.TimeUtil

import scala.collection.mutable
import scala.io.Source

/**
  * Created by li on 16/7/11.
  */
object HotDegreeCalculation {

  /**
    * 统计当前文档库中, 包含社区中提取的关键词的文档数,重复的根据文本ID(url)合并,
    * 特别针对社区(事件)词, 一个社区中包含若干个词, 并且词变化后对应的社区却没有变化.
    *
    * @param fileList 当前文档
    * @param communityWordList textRank提取的每个社区的关键词
    * @return [社区ID, 包含社区中关键词的文档总数]包含社区中关键词的文档总数
    * @author Li Yu
    */
  def communityFrequencyStatistics(fileList: Array[(String, Array[String])],
                                   communityWordList: Array[(String, Array[String])]): Array[(String, Double)] = {

    val communityList = new mutable.HashMap[String, Double]

    communityWordList.foreach {
      line => {

        val item = new mutable.ArrayBuffer[String]
        val communityId  = line._1
        val communityWords  = line._2

        fileList.foreach {
          file => {

            val fileId = file._1
            val fileWordsList = file._2.distinct

            communityWords.foreach { word => {

              if (fileWordsList.contains(word)) item.append(fileId)
            }

              communityList.put(communityId, item.distinct.length)
            }
          }
        }
      }
    }

    communityList.toArray
  }

  /**
    * 使用贝叶斯平均法计算热词候选词的热度
    *
    * @param hotWords 当前热词候选词热度
    * @param preHotWords 前期热词的热度
    * @return 热词候选词和计算出的热度
    * @author Li Yu
    */
  def bayesianAverage(hotWords: Array[(String, Double)],
                      preHotWords: Array[(String, Double)]): mutable.HashMap[String, Double] ={

    val wordLib = hotWords.++(preHotWords)

    //TpSum: 词频和
    val wordLibList = new mutable.ArrayBuffer[(String, Double)]

    wordLib.groupBy(_._1).foreach{
      line =>{
        val temp = line._2.map(_._2).sum
        wordLibList.+=((line._1, temp))
      }
    }
    val wordLibArray = wordLibList.toArray

    //TpAvg:词频和的平均
    val tpSum = wordLibArray.map(_._2).sum
    val tpAvg = tpSum / wordLibArray.length

    //Atp(w)/TpSum 当前词频与词频和比值
    val resultMap = new mutable.HashMap[String, Double]
    val atp = hotWords.toMap
    val wordLibMap = wordLibArray.toMap
    wordLibMap.foreach {
      line =>{
        if (atp.contains(line._1)){
          val temp = atp.get(line._1).get
          val item = temp.toFloat / line._2
          resultMap.put(line._1, item)
        } else {
          resultMap.put(line._1, 0f)
        }
      }
    }

    //R(avg) 当前词频与词频和比值的平均值
    val rAvg = resultMap.values.toArray.sum / resultMap.values.size

    // 热度计算
    val bayesianAverageResult = new mutable.HashMap[String, Double]
    wordLibMap.foreach {
      line => {
        val res1 = wordLibMap.get(line._1).get
        val res2 = resultMap.get(line._1).get
        val value = (res1 * res2 + tpAvg * rAvg) / (res1 + tpAvg)
        bayesianAverageResult.put(line._1, value)
      }
    }

    bayesianAverageResult
  }

  /**
    * 牛顿冷却定律, 使用冷却系数的相反数来反应一个词的热度上升趋势
    *
    * @param hotWords 当前热词候选词
    * @param preHotWords 前一段时间热词候选词
    * @param timeRange 时间间隔
    * @return
    * @author Li Yu
    */
  def newtonCooling(hotWords: Array[(String, Double)],
                    preHotWords: Array[(String, Double)],
                    timeRange: Int): mutable.HashMap[String, Double] ={

    val wordLib = hotWords.++(preHotWords)

    //TpSum: 词频和
    val wordLibList = new mutable.ArrayBuffer[(String, Double)]

    wordLib.groupBy(_._1).foreach{
      line => {
        val temp = line._2.map(_._2).sum
        wordLibList.+=((line._1, temp))
      }
    }

    val wordLibArray = wordLibList.toArray.toMap

    val hotWordsMap = hotWords.toMap

    val newtonCoolingResult = new mutable.HashMap[String, Double]

    wordLibArray.map{
      line => {

        val keywords = line._1
        val tpSum = line._2

        if (hotWordsMap.keySet.contains(keywords)) {

          val atp = hotWordsMap.get(keywords).get.toFloat
          val btp = tpSum - atp
          val item = math.log((atp + 1) / (btp + 1) / timeRange)
          newtonCoolingResult.put(keywords, item)

        } else {

          val btp = tpSum
          val item = math.log((0f + 1) / (btp + 1) / timeRange)
          newtonCoolingResult.put(keywords, item)

        }
      }
    }

    newtonCoolingResult
  }

  /** 将结果保存到本地,将每小时数据保存为一个txt文件.
    *
    * @param dir 文件保存的目录
    * @author Li Yu
    */
  def saveAsTextFile(dir: String, result: Array[(String, Double)]): Unit ={

    val day = TimeUtil.getDay
    val hour = TimeUtil.getCurrentHour

    val writer = new PrintWriter(new File(dir +"%s".format(day) + "-" + "%s".format(hour) + ".txt"))

    for (line <- result) {

      writer.write(line._1 + "\t" + line._2 + "\n")

    }

    writer.close()
  }

  /**
    * 读取当前时间前一个小时的数据,读取本地文件中的结果.
    *
    * @param dir 数据保存的目录
    * @return
    * @author Li Yu
    */
  def readFromFile(dir: String): Array[(String, Double)] ={

    val date = TimeUtil.getPreHourStr

    val temp = Source.fromFile(dir + "%s".format(date) + ".txt" )

    val res = new mutable.ArrayBuffer[(String, Double)]
    temp.getLines().foreach(
      line =>{
        val temp = line.split("\t")
        res.+=((temp(0), temp(1).toDouble))
      }
    )

    res.toArray
  }

  /**
    * 排序算法主程序入口
    *
    * @param dir 当前社区热度的保存路径, 以及读取前一小时的社区热度的读取路径
    * @param fileList 当前采集的文本文档
    * @param communityWordList 社区id 以及社区中包含的关键词
    * @param timeRange 时间间隔 默认为 1
    * @param alpha 贝叶斯平均的权重, 一般为 0.7
    * @param beta 牛顿冷却算法的权重, 一般为 0.3
    * @return 热词候选词和计算出的热度
    * @author Li Yu
    */
  def run(dir: String,
          fileList: Array[(String, Array[String])],
          communityWordList: Array[(String, Array[String])],
          timeRange: Int, alpha: Double, beta: Double): Unit ={

    // 从本地读取前一个小时的社区以及热度
    val preHotWords: Array[(String, Double)] = readFromFile(dir)

    // 计算当前社区对应的热度
    val hotWords: Array[(String, Double)] = communityFrequencyStatistics(fileList, communityWordList)

    //  通过贝叶斯和牛顿冷却,确定两个时间段的热度排序,得到最终的热度结果
    val result = mutable.HashMap[String, Double]()
    val bayesianAverageResult = bayesianAverage(hotWords, preHotWords)
    val newtonCoolingResult = newtonCooling(hotWords, preHotWords, timeRange)

    bayesianAverageResult.foreach {
      line => {
        val key = line._1
        val value = line._2
        val temp = (alpha * value) + beta * newtonCoolingResult.toMap.get(key).get
        result.put(key, temp)
      }
    }
    val res = result.toArray.sortWith(_._2 > _._2)

    // 将最终的热度结果保存到本地文件系统中
    saveAsTextFile(dir, res)
  }

}
