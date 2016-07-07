package meachinelearning.classification

import org.apache.spark.ml.feature.{HashingTF, IDF, StopWordsRemover, Tokenizer}
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source


/**
  * Created by li on 16/4/6.
  * 分别使用非平衡类与平衡类代入贝叶斯模型计算
  */
object BinaryClassification extends App{

  val conf = new SparkConf().setMaster("local").setAppName("StopWordRemove")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  //  val hiveContext = new HiveContext(sc)
  import sqlContext.implicits._

  // DataFrame type 数据集导入
  //  val src = Source.fromFile("/users/li/Intellij/Native-Byes/nativebyes/wordseg_881156.txt").getLines().toArray

  // 总数据集获取未平衡
  //  case class RawDataRecord( category: String ,labels: Double ,text: String)
  //
  //  val src = Source.fromFile("/Users/li/Downloads/traningset/HGHQ.txt").getLines().toArray.map{
  //    line =>
  //      val data = line.split("\t")
  //      RawDataRecord(data(1),data(0).toDouble,data(2))
  //  }

  //  //  平衡数据集获取
  case class RawDataRecord(labels: Double ,text: String)
  val src = Source.fromFile("/Users/li/Downloads/trainingSets/保险").getLines().toArray.map{
    line =>
      val data = line.split("\t")
      RawDataRecord(data(0).toDouble, data(1))
  }

  val srcDF = sqlContext.createDataFrame(src)

  // RDD type
  //    val srcRDD = sc.textFile("/users/li/Intellij/Native-Byes/nativebyes/wordseg_881156.txt").map {
  //      x =>
  //        val data = x.split("\t")
  //        RawDataRecord(data(0),data(1),labels = if(data(1) == "881108" ) 1.0 else 0.0, data(2))
  //    }.toDF()//to DataFrame

  var tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
  var wordsData = tokenizer.transform(srcDF)

  // 去停用词
  // 读取停用词表
  //  val filter = Source.fromFile("/users/li/Intellij/Native-Byes/nativebyes/1.txt" ).getLines().toArray
  val filter = Source.fromFile("/users/li/Intellij/Native-Byes/nativebyes/stop_words_CN" ).getLines().toArray

  val remover = new StopWordsRemover()
    .setInputCol("words")
    .setOutputCol("filtered")
    .setStopWords(filter)

  val removeword = remover.transform(wordsData)


  //70%作为训练数据，30%作为测试数据
  val splits = removeword.randomSplit(Array(0.7, 0.3),seed = 11L)
  //splits.foreach(println)
  var trainingDF = splits(0)
  var testDF = splits(1)



  // 计算文本中的特征维数

  //使用hashingTF计算每个词在文档中的词频
  val hashingTF = new HashingTF().setNumFeatures(100000).setInputCol("filtered").setOutputCol("rawFeatures")
  val featurizedData = hashingTF.transform(trainingDF)
  // println("output2：")
  // featurizedData.select($"category", $"words", $"rawFeatures").foreach(println)
  // featurizedData.show()

  //使用countervertor计算每个词在文档中的词频
  //-------------------------------------
  //  val cvModel: CountVectorizerModel = new CountVectorizer()
  //    .setInputCol("words")
  //    .setOutputCol("rawFeatures")
  //    .setVocabSize(30)
  //    .setMinDF(0) // a term must appear in more or equal to 2 documents to be included in the vocabulary
  //    .fit(wordsData)
  //  val featurizedData = cvModel.transform(wordsData)
  //-------------------------------------


  //计算每个词的TF-IDF
  var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
  val idfModel = idf.fit(featurizedData)
  var rescaledData = idfModel.transform(featurizedData)
  // println("output3：")
  // rescaledData.select($"category", $"features").foreach(println)
  //  rescaledData.select($"labels",$"features").show()

  // 转换成Bayes的输入格式
  var trainDataRdd = rescaledData.select($"labels",$"features").map {
    case Row(label: Double, features: Vector) =>
      LabeledPoint(label , Vectors.dense(features.toArray))
  }
  // println("output4：")
  // trainDataRdd.take(1).foreach(println)

  /** NativeBayes训练模型 */
  val model = NaiveBayes.train(trainDataRdd, lambda = 1.0, modelType = "multinomial")

  /** SVM训练模型 */
  //   var numIterations = 100
  //   val model = SVMWithSGD.train(trainDataRdd , numIterations)

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

  //测试数据集，做同样的特征表示及格式转换
  //  var testwordsData = tokenizer.transform(testDF)
  var testfeaturizedData = hashingTF.transform(testDF)
  //-------------------------------------
  //val testcvModel: CountVectorizerModel = new CountVectorizer()
  // .setInputCol("words")
  // .setOutputCol("rawFeatures")
  // .setVocabSize(30).setMinDF(0)
  // .fit(testwordsData)
  //    val testfeaturizedData = testcvModel.transform(wordsData)
  //--------------------------------------
  var testrescaledData = idfModel.transform(testfeaturizedData)
  // testrescaledData.select($"category", $"features").foreach(println)
  // testrescaledData.show()

  //测试集转换成Bayes训练模型的输入格式
  var testDataRdd = testrescaledData.select($"labels",$"features").map {
    case Row(label: Double , features: Vector) =>
      LabeledPoint(label, Vectors.dense(features.toArray))
  }

  //对测试数据集使用训练模型进行分类预测

  //  朴素贝叶斯分类预测
  //val testpredictionAndLabel = testDataRdd.map(p => (model.predict(p.features), p.label))

  // 支持向量机分类预测
  //model.clearThreshold()
  // Compute raw scores on the test set.
  val predictionAndLabel = testDataRdd.map { point =>
    val predictionpointlabel = model.predict(point.features)
    (predictionpointlabel, point.label)
  }
  //testpredictionAndLabel.foreach(println)

  /** 准确度统计分析 */

  //统计分类准确率
  var testaccuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / testDataRdd.count()
  println("testaccuracy：" + testaccuracy)

  val metrics = new MulticlassMetrics(predictionAndLabel)
  println("Confusion matrix:" + metrics.confusionMatrix)

  // Precision by label
  val label = metrics.labels
  label.foreach { l =>
    println(s"Precision($l) = " + metrics.precision(l))
  }

  // Recall by label
  label.foreach { l =>
    println(s"Recall($l) = " + metrics.recall(l))
  }

  // False positive rate by label
  label.foreach { l =>
    println(s"FPR($l) = " + metrics.falsePositiveRate(l))
  }

  // F-measure by label
  label.foreach { l =>
    println(s"F1-Score($l) = " + metrics.fMeasure(l))
  }
  //
  //  val roc = metrics.roc
  //
  //  // AUROC
  //  val auROC = metrics.areaUnderROC
  //  println("Area under ROC = " + auROC)
  sc.stop()


}

