package topicmodel

import org.apache.spark.mllib.feature
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by li on 16/4/20.
  */
object LDATest {

  val conf = new SparkConf().setAppName("lda").setMaster("local")
  val sc = new SparkContext(conf)


  /**
    * 去除分词结果中的标点符号和停用词
    *
    * @param document 分词结果
    * @param stopWords 停用词
    * @return 返回一个元素为String的Array
    */
  def removeStopWords(document:Array[String], stopWords:Array[String]): Array[String] ={
    if(document != null){
      var res = document.toBuffer
      stopWords.foreach{
        stopWord =>{
          if(res.contains(stopWord)){
            res = res.filterNot(_ == stopWord) //
          }
        }
      }
      res.toArray
    }else{
      null
    }
  }

  /**
    * 计算语料库中的词汇数量,训练集的空间维度
    *
    * @param documents 语料库RDD
    * @return 返回一个整型
    */
  def countWords(documents: RDD[(Double, Array[String])]): Int = {
    val wordCount = documents.flatMap(training => {
      training._2
    }).collect().toSet.size
    wordCount
  }

  /**
    *
    * @param vocabNum 特征空间的维数
    * @param minDocFreq 最小词频
    * @param documents 输入集
    */
  def tfIdf(vocabNum:Int, minDocFreq:Int, documents:RDD[(Double, Array[String])]):Unit = {
    /** tf-idf 计算 */
    // 计算tf
    // vocabNum 计算训练集的维度
    //    val vocabNum = countWords(trainingRDD)
    //    println(vocabNum)
    val hashingTFModel = new feature.HashingTF(vocabNum)
    val docTFRDD = documents.map(line => {
      val temp = hashingTFModel.transform(line._2)
      (line._1, temp)
    })

    // 计算idf
    val idfModel = new IDF(minDocFreq).fit(docTFRDD.map(line => {line._2}))
    val labeedTrainTfIdf = docTFRDD.map( line => {
      val temp = idfModel.transform(line._2)
      LabeledPoint(line._1, temp)
    })
  }

  def main(args:Array[String]){

    val setTextPath = "/Users/li/kunyan/DataSet/LDADatasets"
    val setStopWordPath = "/Users/li/kunyan/DataSet/stop_words_CN"

    // 读取数据
    val documents = sc.textFile(setTextPath).map(_.split("\t")(2)).collect()
    // 读取停用词
    val stopWords = sc.textFile(setStopWordPath).collect()

    val stopwordsremoved = documents.map{
      line =>{
        val item = line.split(",")
        removeStopWords(item,stopWords).mkString(",")
      }
    }

    val termCounts = stopwordsremoved.flatMap(_.map(_ -> 1L)).reduce(_._2 + _._2).collect().

    println(termCounts)


    //  把所有单词组成一个集合,并分配一个id号的map


    // ba把文档doc变成一个稀疏向量,[ID,词频]



  val corpus = stopwordsremoved.zipWithIndex.map(_.swap)

    corpus.foreach(println)





  }




}
