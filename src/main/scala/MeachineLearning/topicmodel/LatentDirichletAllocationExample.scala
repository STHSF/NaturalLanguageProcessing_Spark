import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors

object LatentDirichletAllocationExample {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("LatentDirichletAllocationExample").setMaster("local")
    val sc = new SparkContext(conf)

    // $example on$
    // Load and parse the data
    val data = sc.textFile("/Users/li/kunyan/spark/data/mllib/sample_lda_data.txt")
    data.foreach(println)

    val parsedData = data.map(s => Vectors.dense(s.trim.split(' ').map(_.toDouble)))
    parsedData.foreach(println)

    // Index documents with unique IDs
    val corpus = parsedData.zipWithIndex.map(_.swap).cache()

    // Cluster the documents into three topics using LDA
    val ldaModel = new LDA().setK(3).run(corpus)
    //
    //    // Output topics. Each is a distribution over words (matching word count vectors)
    //    println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):")
    //    val topics = ldaModel.topicsMatrix
    //    for (topic <- Range(0, 3)) {
    //      print("Topic " + topic + ":")
    //      for (word <- Range(0, ldaModel.vocabSize)) { print(" " + topics(word, topic)); }
    //      println()
    //    }
    //
    //    // Save and load model.
    //    ldaModel.save(sc, "/Users/li/kunyan/spark/LatentDirichletAllocationExample/LDAModel")
    //    val sameModel = DistributedLDAModel.load(sc,
    //      "/Users/li/kunyan/spark/LatentDirichletAllocationExample/LDAModel")
    //    // $example off$
    //
    //    sc.stop()
  }
}