package meachinelearning.word2vec

import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by li on 2016/10/17.
  */
object SentimentRun {


  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("textVectors").setMaster("local")
    val sc = new SparkContext(conf)

    // load word2vec model
    val word2vecModelPath = "/Users/li/workshop/DataSet/word2vec/result/2016-07-18-15-word2VectorModel"
    val w2vModel = Word2VecModel.load(sc, word2vecModelPath)

    // load classify model
    val classifyModelPath = "/Users/li/workshop/NaturalLanguageProcessing/src/main/scala/meachinelearning/word2vec/model"
    val classifyModel = SVMModel.load(sc, classifyModelPath)

    // 测试集labeledpoint准备
    val predictSetPath = "/Users/li/workshop/DataSet/trainingsetUnbalance/BXG.txt"
    val predictSet = DataPrepare.readData(predictSetPath)
    val predictSetRdd = sc.parallelize(predictSet)
    val predictSetVec = predictSetRdd.map(row => {
      val x = row.split("\t")
      (x(0), x(1).split(","))})  // 在文章进行分词的情况下，用逗号隔开
      //(x(0), AnsjAnalyzer.cutNoTag(x(1)})  // 如果没有分词，就调用ansj进行分词
      .map(row => (row._1.toDouble, DataPrepare.docVec(w2vModel, row._2)))

    val predictDataRdd = DataPrepare.tagAttacheBatch(predictSetVec)

    //  对测试数据集使用训练模型进行分类预测

    //  朴素贝叶斯分类预测
    //  val testpredictionAndLabel = testDataRdd.map(p => (model.predict(p.features), p.label))

    // 支持向量机分类预测
    // model.clearThreshold()
    // Compute raw scores on the test set.
    val predictionAndLabel = predictDataRdd.map { point =>
      val predictionpointlabel = classifyModel.predict(point.features)
      (predictionpointlabel, point.label)
    }
    //testpredictionAndLabel.foreach(println)

    /** 准确度统计分析 */

    //统计分类准确率
    val testaccuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / predictDataRdd.count()
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

}
