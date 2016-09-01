package wordSegmentation

import org.ansj.domain.Term
import org.ansj.library.UserDefineLibrary
import org.ansj.splitWord.analysis.{NlpAnalysis, ToAnalysis}
import org.apache.spark.SparkContext
import org.nlpcn.commons.lang.tire.domain.Value
import org.nlpcn.commons.lang.tire.library.Library

/**
  * Created by zhangxin on 2016/3/8
  * 基于ansj的分词工具
  */
object AnsjAnalyzer {

  /**
    * ansj分词器初始化, 添加用户词典
    *
    * @param sc  spark程序入口
    * @param userDic 用户词典数组
    * @return 无
    * @author zhangxin
    */
  def init(sc: SparkContext, userDic: Array[String]): Unit = {

    val forest = Library.makeForest("library/default.dic")
    //    val forest = new Forest()

    if(userDic != null ){
      userDic.foreach(addUserDic(_, sc))
    }

  }

  /**
    * 添加用户词典到分词器
    *
    * @param dicPath  词典路径
    * @param sc spark程序入口
    * @return 无
    * @author zhangxin
    */
  def addUserDic(dicPath: String, sc: SparkContext): Unit = {

    //读取词典
    val dic = sc.textFile(dicPath).collect()

    //添加到ansj中
    dic.foreach(UserDefineLibrary.insertWord(_, "userDefine", 100))


  }

  /**
    * 标准分词 ，无词性标注
    *
    * @param sentence  待分词语句
    * @return 分词结果
    * @author zhangxin
    */
  def cutNoTag(sentence: String): Array[String] = {

    val value = new Value("济南\tn")

    Library.insertWord(UserDefineLibrary.ambiguityForest, value)

    //切词
    val sent = ToAnalysis.parse(sentence)

    //提取分词结果，过滤词性
    val words = for(i <- Range(0, sent.size())) yield sent.get(i).getName

    words.toArray
  }

  /**
    * 自然语言分词，带词性标注
    *
    * @param sentence  待分词句子
    * @return  分词结果
    * @author zhangxin
    */
  def cutWithTag(sentence: String): Array[Term] = {

    // 切词
    val sent = NlpAnalysis.parse(sentence)

    // 提取分词结果
    val words = for(i <- Range(0, sent.size())) yield sent.get(i).next()

    words.toArray
  }

}
