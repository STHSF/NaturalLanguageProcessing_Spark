package meachinelearning.word2vec

import java.io.{File, PrintWriter}
//import java.util

import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import util.{TimeUtil, DirectoryUtil, LoggerUtil}

import scala.collection.mutable

/**
  * Created by li on 16/7/8.
  * 使用Word2Vec创建词向量空间, 用于计算词之间的相似度.
  */
object Word2Vec {

  /**
    * 判断文档中是否存在目标内容
    *
    * @param punctuation 目标内容
    * @param text 文档
    * @return
    * @author Li Yu
    * @note rowNum: 6
    */
  def punctuationRemove(punctuation: Array[String], text: Array[String]): Boolean = {

    text.foreach { line => {
      if (punctuation.contains(line)) return true
    }}

    false
  }

  /**
    * 格式转换, 将格式转换成word2Vec需要的输入格式.
    * 1) 去除长度小于3的异常数据
    * 2) 标点符号剔除
    *
    * @param textLib 词库
    * @param punctuation 标点
    * @return
    * @author Li Yu
    * @note rowNum: 6
    */
  def formatTransform(textLib: RDD[String], punctuation: Array[String]): RDD[Seq[String]] = {

    val result = textLib.map(_.split("\t")).filter(_.length > 3)  //去异常值
      .map(x => x(3).split(",")) // 去分词结果
      .map (line => line.filterNot(elem => punctuation.contains(elem)))  // 去标点符号
      .map(_.toSeq)

    result
  }

  /**
    * 训练语料库向量模型
    * 有一些参数在1.5.2中没有, 例如setWindowSize()
    *
    * @param sc SparkContext
    * @param dir Word2VecModel 模型的保存位置
    * @param input 语料库
    * @param seed 搜索深度
    * @param vectorSize 生成的向量长度
    * @param minCount 包含改词的最少文档总数
    * @return
    * @author Li Yu
    * @note rowNum: 8
    */
  def word2VectorModel(sc: SparkContext, dir: String, input: RDD[Seq[String]], seed: Long, vectorSize: Int, minCount: Int): Unit = {

    val word2vec = new Word2Vec()
    LoggerUtil.warn("词向量模型训练开始 》》》》》》》》》》》》》")

    val model = word2vec
      .setSeed(seed)
      .setMinCount(minCount)
      .setVectorSize(vectorSize)
      .fit(input)
    LoggerUtil.warn("模型训练完毕 》》》》》》》》》》》》》")

    val dirFile = new File(dir)

    if (DirectoryUtil.deleteDir(dirFile)) {

      model.save(sc, dir)

    } else {

      model.save(sc, dir)
    }

    LoggerUtil.warn("模型保存完毕 》》》》》》》》》》》》》")
  }

  /**
    * 将Word2VecModel计算的词向量保存到本地
    *
    * @param model Word2VecModel
    * @param dir Word2VecModel模型的保存位置
    * @param modelName 待保存的词向量名
    * @author Li Yu
    * @note rowNum: 7
    */
  def saveVocabularyVectors(model: Word2VecModel, dir: String, modelName: String): Unit = {

    val vectors = model.getVectors.toList
    LoggerUtil.warn("词向量读取结束 》》》》》》》》》》》》》")

    // 保存到本地
    val writer = new PrintWriter(new File(dir +"%s".format(modelName) + ".txt"))

    vectors.foreach {
      line => {
        writer.write("%s\t%s\n".format(line._1, line._2.mkString(",")))
      }
    }

    LoggerUtil.warn("词向量写入结束 》》》》》》》》》》》》")
    writer.close()
  }

  /**
    * 将Word2VecModel计算的词向量保存到HDFS上
    *
    * @param model Word2VecModel
    * @param dir Word2VecModel模型的保存位置
    * @param modelName 待保存的词向量名
    * @author Li Yu
    * @note rowNum: 6
    */
  def saveVocabularyVectors2HDFS(sc: SparkContext, model: Word2VecModel, dir: String, modelName: String): Unit = {

    val vectors = model.getVectors
    LoggerUtil.warn("词向量读取结束 》》》》》》》》》》》》》")

    val vector = vectors.map((line: (String, Array[Float])) => "%s\t%s".format(line._1, line._2.mkString(","))).toList

    // 保存到hdfs上
    val vectorsRDD = sc.parallelize(vector)

    vectorsRDD.saveAsTextFile("dir")

  }

  /**
    * 从词向量库中匹配候选单词的词向量
    * 可能存在的问题, 词库中可能不包含新词,因此新词的词向量是找不到的
    *
    * @param vectorLib 词向量库
    * @param input 候选词
    * @author Li Yu
    * @note rowNum: 10
    */
  def readVocabularyVector(vectorLib: RDD[(String, Array[Double])],
                           input: Array[String]): Map[String, Array[Double]] = {

    val result = new mutable.HashMap[String, Array[Double]]

    input.foreach( word => {

      val temp = vectorLib.filter(x => x._1.contains(word))

      if(temp != null) {

        result.put(temp.first()._1, temp.first()._2)

      } else {

        result.put(word, Array(0.0))

      }
    })

    result.toMap
  }



  def findVocabularyVector(vectorLib: RDD[(String, Array[Double])], word: String)
  :Array[Double] = {


    val temp = vectorLib.filter(x => x._1.contains(word))

    if(temp != null) {

      temp.first()._2
    } else {

      Array(0.0)
    }
  }

  /**
    * 构建词向量
    * 可能存在的问题, 词库中可能不包含新词,因此新词的词向量是找不到的
    *
    * @param model Word2VecModel
    * @param dir Word2VecModel模型的保存位置
    * @param input 待构建词库
    * @return
    * @author Li Yu
    * @note rowNum: 10
    */
  def createW2VMatrix(model: Word2VecModel , dir: String, input: Array[String]): Array[(String, Array[Double])] = {

    val result = new mutable.HashMap[String, Array[Double]]

    input.foreach { line => {

      if (model.getVectors.contains(line)){

        val temp = model.transform(line)
        result.put(line, temp.toArray)

      } else {

        result.put(line, Array(0.0))
      }
    }}

    result.toArray
  }

  /**
    * main函数
    *
    * @param args
    * @author Li Yu
    * @note rowNum: 15
    */
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("word2vec").setMaster("local")
    val sc = new SparkContext(conf)

    // 从hdfs上获取数据
     val input =sc.textFile("/Users/li/Downloads/part-00000")
//    val input =sc.textFile(args(0))
    LoggerUtil.warn("读取hdfs上的数据结束 》》》》》》》》》》》》》")

    // 导入标点符号
    val punctuation = sc.textFile("/Users/li/kunyan/DataSet/punctuations.txt").collect()

    //去异常值, 去标点符号
    val inputData = formatTransform(input, punctuation)
    LoggerUtil.warn("数据转换结束 》》》》》》》》》》》》》文本数量: " + "%s".format(inputData.count()))

    val hour = TimeUtil.getCurrentHour
    val day = TimeUtil.getDay

//    val dir = args(1) + "%s".format(day) + "-" + "%s".format(hour) + "-word2VectorModel"
     val dir = "/Users/li/kunyan/DataSet/word2vec/result/" + "%s".format(day) + "-" + "%s".format(hour) + "-word2VectorModel"

    // 创建词向量模型
//    val r1 = args(2).toLong
//    val r2 = args(3).toInt
//    val r3 = args(4).toInt
    val r1 = 10
    val r2 = 100
    val r3 = 50
    word2VectorModel(sc, dir, inputData, r1, r2, r3)
    LoggerUtil.warn("词典构建结束 》》》》》》》》》》》》》")

    val model = Word2VecModel.load(sc, dir)
    LoggerUtil.warn("load模型成功 》》》》》》》》》》》》》")

    //保存词向量
    saveVocabularyVectors(model, dir, day)
    LoggerUtil.warn("词向量保存成功 》》》》》》》》》》》》》")

    sc.stop()
    println("-----------程序结束--------------")

  }

}
