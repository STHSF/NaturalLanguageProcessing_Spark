import meachinelearning.word2vec.Word2Vec
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by li on 16/7/15.
  */
object word2vecTest {


  def main(args: Array[String]) {


    val conf = new SparkConf().setAppName("word2vec").setMaster("local")
    val sc = new SparkContext(conf)

    val data = sc.parallelize(List("sadfad\tsdfasdfasdf\tasdfasdfasdfasdfasdf\t中欧,8,美国,成都,;,", "dddddd\tfdasdfvvv\tdfafasfdsadfs\t日本,中欧,.,中国,加州,/,顺分"))

    val punctuation = sc.textFile("/Users/li/kunyan/DataSet/punctuations.txt").collect()

    val s =  Word2Vec.formatTransform(data, punctuation)

    s.foreach(println)

  }

}
