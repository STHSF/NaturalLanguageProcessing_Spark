


/**
  * Created by li on 16/4/15.
  *
  *
  *
  *     import org.apache.spark.ml.feature.Word2Vec

  */
object Test {

  def main(args: Array[String]) {
    //    val setPath = "/Users/li/kunyan/DataSet/trainingsetUnbalance/YSJS.txt"
    //    val industry = "化工化纤"
    //    BinaryClassificationRDD.dataOperation(setPath, industry)
    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)


    // Input data: Each row is a bag of words from a sentence or document.
    val documentDF = sqlContext.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")

    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val model = word2Vec.fit(documentDF)
    val result = model.transform(documentDF)
    result.select("result").foreach(println)
    result.show()

  }

}
