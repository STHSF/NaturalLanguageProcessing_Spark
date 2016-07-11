package meachinelearning.hotdegreecalculate

import com.sun.tools.javac.util.ListBuffer

import scala.collection.mutable

/**
  * Created by li on 16/7/5.
  * 计算社区热度的功能模块, 揉合了社区热词的热度计算,
  */
object CommunityFrequencyStatistics {

  /**
    * 统计当前文档库中, 包含社区中提取的关键词的文档数,重复的根据文本ID(url)合并,
    * 特别针对社区(事件)词, 一个社区中包含若干个词, 并且词变化后对应的社区却没有变化.
    *
    * @param fileList 当前文档
    * @param communityWordList textRank提取的每个社区的关键词
    * @return [社区ID, 包含社区中关键词的文档总数]包含社区中关键词的文档总数
    */
  def communityFrequencyStatistics(fileList: Array[(String, Array[String])],
                                   communityWordList: Array[(String, Array[String])]): Array[(String, Double)] = {

    val communityList = new mutable.HashMap[String, Double]

    communityWordList.foreach {
      line => {

        val item = new ListBuffer[String]
        val communityId  = line._1
        val communityWords  = line._2

        fileList.foreach { file => {

            val fileId = file._1
            val fileWordsList= file._2.distinct

            communityWords.foreach { word => {

                if (fileWordsList.contains(word)) item.append(fileId)
              }

                communityList.put(communityId, item.toArray().distinct.length)
            }
          }
        }
      }
    }

    communityList.toArray
  }

}
