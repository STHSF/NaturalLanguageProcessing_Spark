package meachinelearning.word2vec

import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, mllib}

import scala.collection.mutable

/**
  * Created by li on 16/7/8.
  */
object word2Vec {

  /**
    * 格式转换, 将格式转换成word2Vec需要的输入格式.
    *
    * @param textLib
    * @return
    * @author Li Yu
    */
  def formatTransform(textLib: RDD[(String, Array[String])]): RDD[Seq[String]] = {

    val result = textLib.values.map(_.toSeq.distinct)

    result
  }

  /**
    * 训练语料库向量模型
    *
    * @param input 语料库
    * @return
    * @author Li Yu
    */
  def word2VectorModel(input: RDD[Seq[String]], seed: Long, vectorSize: Int): Word2VecModel = {

    val word2vec = new Word2Vec

    val model = word2vec
      .setSeed(seed)
      .setVectorSize(vectorSize)
      .fit(input)

    model
  }

  /**
    * 构建词向量
    *
    * @param model Word2VecModel
    * @param input 待构建词库
    * @return
    * @author Li Yu
    */
  def createW2VMatrix(model: Word2VecModel, input: Array[String]): Array[(String, mllib.linalg.Vector)] = {

    val result = new mutable.HashMap[String, mllib.linalg.Vector]

    input.foreach{
      line =>{
        val temp = model.transform(line)
        result.put(line, temp)
      }
    }

    result.toArray
  }





  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("word2vec").setMaster("local")
    val sc = new SparkContext(conf)

    val input = sc.textFile("text8").map(x => x.split(" ").toSeq)

    val model = word2VectorModel(input, 20, 100)
    
    val synonyms = model.findSynonyms("china", 40)

    for ((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
    }

  }


}
