package meachinelearning.word2vec

import java.io.File

import util.{DirectoryUtil, JSONUtil}
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by li on 2016/10/13.
  *
  */
object ClassifyModel {


  def classify(trainDataRdd: RDD[LabeledPoint]): SVMModel = {

    /** NativeBayes训练模型 */
    //  val model = NaiveBayes.train(trainDataRdd, lambda = 1.0, modelType = "multinomial")

    /** SVM训练模型 */
    val numIterations = 500
    val model = SVMWithSGD.train(trainDataRdd , numIterations)

    /** RandomForest训练模型 */
    //    val numClasses = 2
    //    val categoricalFeaturesInfo = Map[Int, Int]()
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

    val jsonPath = "/Users/li/workshop/NaturalLanguageProcessing/src/main/scala/meachinelearning/word2vec/twc/W2VJsonConf.json"

    JSONUtil.initConfig(jsonPath)

    val word2vecModelPath = JSONUtil.getValue("w2v", "w2vmodelPath")
    val modelSize = JSONUtil.getValue("w2v", "w2vmodelSize").toInt
    val isModel = JSONUtil.getValue("w2v", "isModel").toBoolean

    //  val word2vecModelPath = "hdfs://master:9000/home/word2vec/classifyModel-10-100-20/2016-08-16-word2VectorModel"
    val w2vModel = Word2VecModel.load(sc, word2vecModelPath)

    // 构建训练集的labeledpoint格式
    // val trainSetPath = "/Users/li/workshop/DataSet/trainingsetUnbalance/BXX.txt"
    // val trainSetPath = "/Users/li/workshop/DataSet/trainingsetUnbalance/BXX.txt"
    val trainSetPath = "/Users/li/workshop/DataSet/trainingSets/计算机"

    val trainSet = DataPrepare.readData(trainSetPath)
    val trainSetRdd = sc.parallelize(trainSet).cache()
    //val trainSetRdd = sc.textFile(trainSetPath)

    // val trainSetVec = trainSetRdd.map( row => {
    // val x = row.split("\t")
    //  (x(0), x(1).split(","))})  // 在文章进行分词的情况下，用逗号隔开
    //  //(x(0), AnsjAnalyzer.cutNoTag(x(1)})   // 如果没有分词，就调用ansj进行分词
    //  .map(row => (row._1.toDouble, DataPrepare.docVec(w2vModel, row._2)))

    val trainDataRdd = TextVectors.textVectorsWithWeight(trainSetRdd, w2vModel, modelSize, isModel).cache()

    val classifyModel = classify(trainDataRdd)

    val classifyModelPath = JSONUtil.getValue("classify", "classifymodelpath")
    DirectoryUtil.deleteDir(new File(classifyModelPath))
    classifyModel.save(sc, classifyModelPath)
    println("分类模型保存完毕。")

  }
}
