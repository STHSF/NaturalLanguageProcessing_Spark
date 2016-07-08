package meachinelearning.word2vec

import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by li on 16/7/8.
  */
object word2Vec {


  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("word2vec").setMaster("local")
    val sc = new SparkContext(conf)

    val input = sc.textFile("text8").map(x => x.split(" ").toSeq)

    val word2vec = new Word2Vec
    val model = word2vec.fit(input)

    val synonyms = model.findSynonyms("china", 40)

    for ((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
    }

  }


}
