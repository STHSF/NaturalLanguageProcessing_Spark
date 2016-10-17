package meachinelearning.word2vec

import breeze.linalg._
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by li on 16/7/14.
  * word2vec模型
  */
object textVectors {


  /**
    * 生成文本空间向量, 文本中每个单词的词向量的加权平均。
    * @param text 文本数组
    * @param model Word2Vec模型
    * @param size Word2Vec模型中词向量的长度
    * @return
    */
  def textVectors(text: Array[String], model: Word2VecModel, size: Int): Vector[Double] = {

    val wordVectors = Vector.zeros[Double](size)
    var docVectors = Vector.zeros[Double](size)
    var vector = Array[Double](size)
    var count = 0.0
    for (word <- text.indices) {

      try {
        vector = model.transform(text(word)).toArray
      }
      catch {
        case e: Exception => vector = Vector.zeros[Double](size).toArray
      }

      val tmp = Vector.apply(vector)
      wordVectors.+=(tmp)
      count += 1.0
    }

    if(count != 0) {

      // println(count)
      docVectors = wordVectors./=(count)
    }

    docVectors
  }

  def textVectorsTest(): Unit = {

    val conf = new SparkConf().setAppName("textVectors").setMaster("local")
    val sc = new SparkContext(conf)

    // 保存在hdfs上的模型的路径
    val dir = "hdfs://master:9000/home/word2vec/model-10-100-20/2016-08-16-word2VectorModel"
    // val dir = "hdfs://master:9000/home/word2vec/model20160830-10-100-20/2016-08-31-word2VectorModel"

    // 读取保存在hdfs上的模型
    val model = Word2VecModel.load(sc, dir)

    // word2vec model test
    val synonyms = model.findSynonyms("共产党", 100)
    for((synonym, cosineSimilarity ) <- synonyms){
    // println(s"$synonym   $cosineSimilarity")  // AQSW
      println(s"$synonym")
    }

    //    val text = Array("大阴棒", "jijiji", "大阴")
    //    val res = textVectors(text, model, 100)
    //    println(res)
  }

  def main(args: Array[String]) {

    textVectorsTest()
  }

}
