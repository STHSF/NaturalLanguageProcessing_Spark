package ml.classification

import org.apache.spark.ml.feature.{HashingTF, IDF, StopWordsRemover, Tokenizer}
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.feature.PCA
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by li on 16/4/8.
  */
object BinaryClassificationWithPCA extends App {

  val conf = new SparkConf().setMaster("local").setAppName("StopWordRemove")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
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
  val src = sc.textFile("/Users/li/kunyan/DataSet/trainingSets/保险").map{
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
  val filter = Source.fromFile("/Users/li/kunyan/DataSet/stop_words_CN" ).getLines().toArray

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



  //使用hashingTF计算每个词在文档中的词频
  val hashingTF = new HashingTF().setNumFeatures(5000).setInputCol("filtered").setOutputCol("rawFeatures")
  val featurizedData = hashingTF.transform(trainingDF)
  // println("output2：")
  // featurizedData.select($"category", $"words", $"rawFeatures").foreach(println)
  // featurizedData.show()


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
  }.cache()

//trainDataRdd.foreach(println)


  /** pca降维 */
//  val pca = new PCA(trainDataRdd.first().features.size/2).fit(trainDataRdd.map(_.features))
  val pca = new PCA(1000).fit(trainDataRdd.map(_.features))

  val training_pca = trainDataRdd.map(p => p.copy(features = pca.transform(p.features)))


  /** NativeBayes训练模型 */
//  val modelPCA = NaiveBayes.train(training_pca, lambda = 1.0, modelType = "multinomial")

  /** SVM训练模型 */
   var numIterations = 100
   val modelPCA = SVMWithSGD.train(training_pca , numIterations)


  /** 测试集数据处理 */
  //测试数据集，做同样的特征表示及格式转换
  //  var testwordsData = tokenizer.transform(testDF)
  var testfeaturizedData = hashingTF.transform(testDF)
  var testrescaledData = idfModel.transform(testfeaturizedData)
  // testrescaledData.select($"category", $"features").foreach(println)
  // testrescaledData.show()

  //测试集转换成Bayes训练模型的输入格式
  var testDataRdd = testrescaledData.select($"labels",$"features").map {
    case Row(label: Double , features: Vector) =>
      LabeledPoint(label, Vectors.dense(features.toArray))
  }

  val test_pca = testDataRdd.map(p => p.copy(features = pca.transform(p.features)))

  val predictionAndLabel_pca = test_pca.map { point =>
    val predictionpointlabel = modelPCA.predict(point.features)
    (predictionpointlabel, point.label)
  }


  /** 准确度统计分析 */

  //统计分类准确率
  var accuracy = 1.0 * predictionAndLabel_pca.filter(x => x._1 == x._2).count() / testDataRdd.count()
  println("Accuracy：" + accuracy)

//  val metrics = new MulticlassMetrics(predictionAndLabel_pca)
//  println("Confusion matrix:" + metrics.confusionMatrix)
//
//  // Precision by label
//  val label = metrics.labels
//  label.foreach { l =>
//    println(s"Precision($l) = " + metrics.precision(l))
//  }
//
//  // Recall by label
//  label.foreach { l =>
//    println(s"Recall($l) = " + metrics.recall(l))
//  }
//
//  // False positive rate by label
//  label.foreach { l =>
//    println(s"FPR($l) = " + metrics.falsePositiveRate(l))
//  }
//
//  // F-measure by label
//  label.foreach { l =>
//    println(s"F1-Score($l) = " + metrics.fMeasure(l))
//  }
//  //
//  //  val roc = metrics.roc
//  //
//  //  // AUROC
//  //  val auROC = metrics.areaUnderROC
//  //  println("Area under ROC = " + auROC)
//  sc.stop()

}
