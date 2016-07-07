import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.feature._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by li on 16/4/13.
  * 使用PCA降维,出现的问题是mllib中的PCA目前对输入数据的维度进行了限制,
  * 只能输入维数小于65535的数据集,大于这个维度会抛出异常
  *
  */
object BinaryClassificationRDDWithPCA {

  val conf = new SparkConf().setAppName("pca").setMaster("local")
  val sc = new SparkContext(conf)


//  def getFiles(filePath: String ,index: String): RDD[(String, String)]={
//
//
//    index match {
//      case "train" => {
//        val file = sc.textFile(filePath).map {
//          line =>
//            val res = line.split("\t")
//            (res(0),res(1))
//        }
//      }
//
//      case "test" => {
//
//      }
//    }
//
//
//    }


  /**
    * 统计文档中的单词数
    *
    * @param content 文档内容
    * @return
    */
  def countWord(content: RDD[(Double, Array[String])]): Int ={
    val wordCount = content.flatMap{ line =>{
      line._2
    }}.collect().toSet.size
    wordCount
  }

  /**
    *去停用词
    *
    * @param content 需要去停的内容
    * @param stopWords 停用词词典
    * @return 去停后的内容
    */
  def stopWordRemove(content:Array[String], stopWords: Array[String]): Array[String] = {
    var res = content.toBuffer
    if (res != null) {
      stopWords.foreach {
        stopWord =>{
          if (res.contains(stopWord)) {
            res = res.filterNot(_ == stopWord)
          }
        }
      }
      res.toArray
    } else {
      null
    }
  }






  def main(args:Array[String]) {

    val parasDoc = 1
    val parasFeatures = 1000
    val setTextPath = "/Users/li/kunyan/DataSet/trainingSets/非银金融"
    val setStopWordPath = "/Users/li/kunyan/DataSet/stop_words_CN"

    /** data import*/
    // unbalanced dataset
    //    val resRDD = sc.textFile(setTextPath).map{
    //      line =>
    //        val item = line.split("\t")
    //        (item(0), item(2))
    //    }.cache()

    // balanced dataset
    val resRDD = sc.textFile(setTextPath).map{
      line =>
        val data = line.split("\t")
        //      RawDataRecord.+=((data(0), data(1)))
        (data(0), data(1))
    }.cache()
    /** stopwords remove */
    val stopWords = sc.textFile(setStopWordPath).collect()

    val stopWordRemoved = resRDD.map{
      line =>
        val item = line._2.split(",")
        (line._1.toDouble, stopWordRemove(item, stopWords))
    }

    /** trainingset testset split*/
    val splits = stopWordRemoved.randomSplit(Array(0.9, 0.1),seed = 11L)
    val trainSet = splits(0)
    val testSet = splits(1)

    /** tf-idf compute*/
    val vocabNum = countWord(trainSet)
    println(vocabNum)
    // tf
    val tfModel = new HashingTF(vocabNum)
    val trainSetTf = trainSet.map{ line =>{
      val item = tfModel.transform(line._2)
      (line._1, item)
    }
    }

    // idf
    val IdfModel = new IDF(parasDoc).fit(trainSetTf.map(line => {line._2}))
    val trainSetTfIdf = trainSetTf.map{
      line =>
        LabeledPoint(line._1, IdfModel.transform(line._2))
    }.cache()


    /** PCA 降维 */

    //    val a = trainSetTfIdf.map(_.features)
    val pcaModel = new PCA(vocabNum/2).fit(trainSetTfIdf.map(_.features))
    val trainProjected = trainSetTfIdf.map(line =>line.copy(features = pcaModel.transform(line.features)))

    /** 模型训练*/
    // val byesModel = NaiveBayes.train(trainProjected, 1.0, "multinomial")

    /** SVM训练模型 */
    val numIterations = 10
    val modelPCA = SVMWithSGD.train(trainProjected, numIterations)


    /**测试集数据处理*/
    val testSetTf = testSet.map{
      line =>
        (line._1, tfModel.transform(line._2))
    }
    val testSetTfIdf = testSetTf.map{
      line =>
        LabeledPoint(line._1.toDouble, IdfModel.transform(line._2))
    }
    //      val selectedTest = testSetTfIdf.map(line => {
    //        val temp = chiSqSelectorModel.transform(line.features)
    //        LabeledPoint(line.label, temp)
    //      })

    val testProjected = testSetTfIdf.map(p =>p.copy(features = pcaModel.transform(p.features)))

    //
    /**模型预测*/
    val predictionAndLabels = testProjected.map {
      line =>
        val prediction = modelPCA.predict(line.features)
        //      println((prediction, line.label))
        (prediction, line.label)
    }



    /** 模型评价 */
    val metrics = new MulticlassMetrics(predictionAndLabels)
    println("Confusion matrix:")
    println(metrics.confusionMatrix)

    // Precision by label
    val labels = metrics.labels
    labels.foreach { l =>
      println(s"Precision($l) = " + metrics.precision(l))
    }

    // Recall by label
    labels.foreach { l =>
      println(s"Recall($l) = " + metrics.recall(l))
    }

    // False positive rate by label
    labels.foreach { l =>
      println(s"FPR($l) = " + metrics.falsePositiveRate(l))
    }

    // F-measure by label
    labels.foreach { l =>
      println(s"F1-Score($l) = " + metrics.fMeasure(l))
    }
    (metrics.precision(1.0), metrics.recall(1.0))
  }
  //

}

