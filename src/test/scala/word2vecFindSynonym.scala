import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by li on 16/7/14.
  */
object word2vecFindSynonym {

  val conf = new SparkConf().setAppName("findsynonym").setMaster("local")
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {

    // 保存在hdfs上的模型的路径
    val dir = "hdfs://61.147.80.245:9000/home/word2vec/model3/2016-07-25-word2VectorModel"

    // 读取保存在hdfs上的模型
    val model = Word2VecModel.load(sc, dir)

    val synonyms = model.findSynonyms("傻", 40)

    for((synonym, cosineSimilarity ) <- synonyms){
      println(s"$synonym   $cosineSimilarity")
    }

  }

}
