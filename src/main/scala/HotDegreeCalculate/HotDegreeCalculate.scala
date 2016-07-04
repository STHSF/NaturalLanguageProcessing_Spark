package hotdegreecalculate

import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * Created by li on 16/7/4.
  */
object HotDegreeCalculate {

  /**
    * 使用贝叶斯平均法计算热词候选词的热度
    * @param hotWords 当前热词候选词热度
    * @param preHotWords 前期热词的热度
    * @return 热词候选词和计算出的热度
    * @author Li Yu
    */
  def bayesianAverage(hotWords: RDD[(String, Int)],
                      preHotWords: RDD[(String, Int)]): mutable.HashMap[String, Float] ={

    val wordLib = hotWords.++(preHotWords)

    //TpSum: 词频和
    val wordLibArray = wordLib.reduceByKey(_ + _).collect()

    //TpAvg:词频和的平均
    val tpSum = wordLibArray.map(_._2).sum
    val tpAvg = tpSum.toFloat / wordLibArray.length

    //Atp(w)/TpSum 当前词频与词频和比值
    val resultMap = new mutable.HashMap[String, Float]
    val atp = hotWords.collect().toMap
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
    val bayesianAverageResult = new mutable.HashMap[String, Float]
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
    * @param hotWords 当前热词候选词
    * @param preHotWords 前一段时间热词候选词
    * @param timeRange 时间间隔
    * @return
    * @author Li Yu
    */
  def newtonCooling(hotWords: RDD[(String, Int)],
                    preHotWords: RDD[(String, Int)],
                    timeRange: Int): mutable.HashMap[String, Float] ={

    val wordLib = hotWords.++(preHotWords)

    //TpSum: 词频和
    val wordLibArray = wordLib.reduceByKey(_ + _).collect().toMap

    val newtonCoolingResult = new mutable.HashMap[String, Float]

    wordLibArray.map{
      line => {
        val keywords = line._1
        val atp = line._2
        val btp = wordLibArray.get(keywords).get - atp
        val item = math.log((atp + 1) / (btp + 1) / timeRange).toFloat
        newtonCoolingResult.put(keywords, item)
      }
    }

    newtonCoolingResult
  }

  /**
    * 排序算法主程序入口
    * @param hotWords 当前热词
    * @param preHotWords 前段时间的热词
    * @param timeRange 时间间隔
    * @param alpha 贝叶斯平均的权重, 一般0.7
    * @param beta 牛顿冷却算法的权重, 一般0.3
    * @return 热词候选词和计算出的热度
    * @author Li Yu
    */
  def run(hotWords: RDD[(String, Int)],
          preHotWords: RDD[(String, Int)],
          timeRange: Int, alpha: Double,
          beta: Double): Array[(String, Float)] ={

    val result = mutable.HashMap[String, Float]()

    val bayesianAverageResult = bayesianAverage(hotWords, preHotWords)

    val newtonCoolingResult = newtonCooling(hotWords, preHotWords, timeRange).toMap

    bayesianAverageResult.foreach {
      line => {
        val key = line._1
        val value = line._2
        val temp = (alpha * value) + beta * newtonCoolingResult.get(key).get
        result.put(key, temp.toFloat)
      }
    }

    result.toArray
  }

}