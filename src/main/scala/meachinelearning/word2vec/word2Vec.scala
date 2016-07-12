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
    * 有一些参数在1.5.2中没有例如setWindowSize()
    *
    * @param sc SparkContext
    * @param dir Word2VecModel 模型的保存位置
    * @param input 语料库
    * @return
    * @author Li Yu
    */
  def word2VectorModel(sc: SparkContext, dir: String, input: RDD[Seq[String]], seed: Long, vectorSize: Int): Unit = {

    val word2vec = new Word2Vec()

    val model = word2vec
      .setSeed(seed)
      .setVectorSize(vectorSize)
      .fit(input)

    model.save(sc, dir)
  }




  /**
    * 构建词向量
    *
    * @param sc SparkContext
    * @param dir Word2VecModel 模型的保存位置
    * @param input 待构建词库
    * @return
    * @author Li Yu
    */

  def createW2VMatrix(sc: SparkContext, dir: String, input: Array[String]): Array[(String, mllib.linalg.Vector)] = {

    val model = Word2VecModel.load(sc, dir)

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

    val input = sc.textFile("/Users/li/Downloads/text8").map(x => x.split(" ").toSeq)

    println("input" + input.collect().size )
    val dir = "/Users/li/kunyan/DataSet/word2vec/result/word2VectorModel"

//    word2VectorModel(sc, dir, input, 20, 100)


    val synonyms = Word2VecModel.load(sc, dir).findSynonyms("fuck", 40)

    for ((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
    }

    val res = Word2VecModel.load(sc, dir).getVectors

//      .transform("england")
//    println(res)
    println(res.size)


  }



}
