package meachinelearning.hotdegreecalculate

import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * Created by li on 16/7/5.
  * 计算社区热度的功能模块, 揉合了社区热词的热度计算,
  */
object CommunityFrequencyStatistics {


  /**
    * 筛选出出现了社区内词的所有文章
    *
    * @param communityWords 社区中的词
    * @param textWords 新闻
    * @return Boolean 新闻中存在社区中的词返回true
    * @author Li Yu
    * @note rowNum: 11
    */
  def filterFunc(communityWords: Array[String],
                 textWords: Array[String]): Boolean = {

    communityWords.foreach {
      word => {

        if (textWords.contains(word)) {

          return true
        }
      }
    }

    false
  }

  /**
    * 统计当前文档库中, 包含社区中提取的关键词的文档数,重复的根据文本ID(url)合并,
    * 特别针对社区(事件)词, 一个社区中包含若干个词, 并且词变化后对应的社区却没有变化.
    *
    * @param fileList 当前文档
    * @param communityWordList textRank提取的每个社区的关键词
    * @return [社区ID, 包含社区中关键词的文档总数]包含社区中关键词的文档总数
    * @author Li Yu
    * @note rowNum: 13
    */
  def communityFrequencyStatisticsRDD(fileList: RDD[Array[String]],
                                  communityWordList: Array[(String, Array[String])]): Array[(String, Double)] = {

    val communityList = new mutable.HashMap[String, Double]

    communityWordList.foreach {
      community => {

        val communityID = community._1
        val communityWords = community._2
        val temp = fileList.filter(content => filterFunc(communityWords, content)).count().toDouble

        communityList.+=((communityID, temp))
      }
    }

    communityList.toArray
  }


  /**
    * 统计当前文档库中, 包含社区中提取的关键词的文档数,重复的根据文本ID(url)合并,
    * 特别针对社区(事件)词, 一个社区中包含若干个词, 并且词变化后对应的社区却没有变化.
    *
    * @param fileList 当前文档
    * @param communityWordList textRank提取的每个社区的关键词
    * @return [社区ID, 包含社区中关键词的文档总数]包含社区中关键词的文档总数
    * @author Li Yu
    * @note rowNum: 22
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

}


