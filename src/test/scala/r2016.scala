import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by li on 16/7/14.
  */
object r2016 {


  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("map flatMap").setMaster("local")
    val sc = new SparkContext(conf)

    val str = Array("iii ewewew ddd ddfddf dddd ddddas weewwe wewer fdggfg ewterewt", "asfas fgrg jgiuiog hgukguy hjguy bhkui")
//
//    val res1 = str.map(x => x.split(" "))
//
//    val res2 = str.flatMap(x => x.split(" "))

//    val dir = "/Users/li/kunyan/DataSet/word2vec/result/2016-07-18-15-word2VectorModel"
    val dir = "/Users/li/kunyan/DataSet/word2vec/result/2016-07-18-14-word2VectorModel"


    val res = sc.textFile("/Users/li/kunyan/DataSet/word2vec/result/2016-07-18-15-word2VectorModel2016-07-18.txt")

    println(res.count())


    val model = Word2VecModel.load(sc, dir)

    val synonyms = model.findSynonyms("傻", 40)

    for((synonym, cosineSimilarity ) <- synonyms){
      println(s"$synonym   $cosineSimilarity")
    }

  }

}
