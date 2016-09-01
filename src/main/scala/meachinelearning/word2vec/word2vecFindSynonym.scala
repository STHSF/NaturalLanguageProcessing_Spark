package meachinelearning.word2vec

import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by li on 16/7/14.
  * word2vec模型
  */
object word2vecFindSynonym {

  val conf = new SparkConf().setAppName("findsynonym").setMaster("local")
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {

    // 保存在hdfs上的模型的路径
//    val dir = "hdfs://61.147.114.85:9000/home/word2vec/model3/2016-07-25-word2VectorModel"
//    val dir = "hdfs://61.147.114.85:9000/home/word2vec/model-10-100-20/2016-08-16-word2VectorModel"
    val dir = "hdfs://61.147.114.85:9000/home/word2vec/model20160830-10-100-20/2016-08-31-word2VectorModel"

    // 读取保存在hdfs上的模型
    val model = Word2VecModel.load(sc, dir)

    val synonyms = model.findSynonyms("大阴棒", 100)

    for((synonym, cosineSimilarity ) <- synonyms){

//      println(s"$synonym   $cosineSimilarity")  AQSW
      println(s"$synonym")
    }

  }

}
