package meachinelearning.word2vec

import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by li on 2016/10/13.
  *
  */
object SentimentModel {


  def classify(trainDataRdd: RDD[LabeledPoint]): SVMModel = {

    /** NativeBayes训练模型 */
    //  val model = NaiveBayes.train(trainDataRdd, lambda = 1.0, modelType = "multinomial")

    /** SVM训练模型 */
    val numIterations = 100

    val model = SVMWithSGD.train(trainDataRdd , numIterations)

    /** RandomForest训练模型 */
    //    val numClasses = 2
    //      val categoricalFeaturesInfo = Map[Int, Int]()
    //    val numTrees = 3
    //    val featureSubsetStrategy = "auto"
    //    val impurity = "gini"
    //    val maxDepth = 4
    //    val maxBins = 32
    //    val model = RandomForest.trainClassifier(trainDataRdd, numClasses, categoricalFeaturesInfo,numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    /** GradientBoostedTrees训练模型 */
    //    // Train a GradientBoostedTrees model.
    //    // The defaultParams for Classification use LogLoss by default.
    //    val boostingStrategy = BoostingStrategy.defaultParams("Classification")
    //    boostingStrategy.numIterations = 3 // Note: Use more iterations in practice.
    //    boostingStrategy.treeStrategy.numClasses = 2
    //    boostingStrategy.treeStrategy.maxDepth = 5
    //    // Empty categoricalFeaturesInfo indicates all features are continuous.
    //    boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()
    //
    //    val model = GradientBoostedTrees.train(trainDataRdd, boostingStrategy)

    model

    }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("textVectors").setMaster("local")
    val sc = new SparkContext(conf)

    val word2vecModelPath = "/Users/li/workshop/DataSet/word2vec/result/2016-07-18-15-word2VectorModel"
    val w2vModel = Word2VecModel.load(sc, word2vecModelPath)

    // 训练集labeledpoint准备
    val trainSetPath = "/Users/li/workshop/DataSet/trainingsetUnbalance/BXX.txt"
    val trainSet = DataPrepare.readData(trainSetPath)
    val trainSetRdd = sc.parallelize(trainSet)
    val trainSetVec = trainSetRdd.map(row => {
      val x = row.split("\t")
      (x(0), x(1).split(","))})
      //(x(0), AnsjAnalyzer.cutNoTag(x(1)})
      .map(row => (row._1.toDouble, DataPrepare.docVec(w2vModel, row._2)))

    val trainDataRdd = DataPrepare.tagAttacheBatch(trainSetVec)

    val model = classify(trainDataRdd)
    val modelPath = "/Users/li/workshop/NaturalLanguageProcessing/src/main/scala/meachinelearning/word2vec/model"
    model.save(sc, modelPath)

  }
}
