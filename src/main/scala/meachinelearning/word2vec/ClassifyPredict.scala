package meachinelearning.word2vec

import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import util.JSONUtil
import wordSegmentation.AnsjAnalyzer

/**
  * Created by li on 2016/10/17.
  */
object ClassifyPredict {


  /**
    * 准确度统计分析
    *
    * @param predictionAndLabel
    */
  def acc(predictionAndLabel: RDD[(Double, Double)],
          predictDataRdd: RDD[LabeledPoint]): Unit = {

    //统计分类准确率
    val testAccuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / predictDataRdd.count()
    println("testAccuracy：" + testAccuracy)

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

    // val roc = metrics.roc

    // // AUROC
    // val auROC = metrics.areaUnderROC
    // println("Area under ROC = " + auROC)

  }


  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("textVectors").setMaster("local")
    val sc = new SparkContext(conf)

    val jsonPath = "/Users/li/workshop/NaturalLanguageProcessing/src/main/scala/meachinelearning/word2vec/twc/W2VJsonConf.json"

    JSONUtil.initConfig(jsonPath)

    val word2vecModelPath = JSONUtil.getValue("w2v", "w2vmodelPath")
    val modelSize = JSONUtil.getValue("w2v", "w2vmodelSize").toInt
    val isModel = JSONUtil.getValue("w2v", "isModel").toBoolean
    // load word2vec model
    val w2vModel = Word2VecModel.load(sc, word2vecModelPath)

    // load classify model
    val classifyModelPath = JSONUtil.getValue("classify", "classifymodelpath")
    val classifyModel = SVMModel.load(sc, classifyModelPath)

    // 构建测试集labeledpoint格式
    val predictSetPath = "/Users/li/workshop/DataSet/trainingSets/test"
    val predictSet = DataPrepare.readData(predictSetPath)
    val predictSetRdd = sc.parallelize(predictSet)

//    // 对于单篇没有分词的文章
    val predictSetVec = predictSetRdd.map(row => {
      (1, AnsjAnalyzer.cutNoTag(row))
    }).map(row => (row._1.toDouble, DataPrepare.docVec(w2vModel, row._2, modelSize)))
//    val predictDataRdd = DataPrepare.tagAttacheBatchWhole(predictSetVec)

    val predictDataRdd = TextVectors.textVectorsWithWeight(predictSetRdd, w2vModel, modelSize, isModel).cache()

    /** 对测试数据集使用训练模型进行分类预测 */
    // classifyModel.clearThreshold()
    // Compute raw scores on the test set.
    val predictionAndLabel = predictDataRdd.map{ point => {
      val predictionFeature = classifyModel.predict(point.features)
      (predictionFeature, point.label)
    }}

    // 准确度统计分析
    acc(predictionAndLabel, predictDataRdd)
    //predictionAndLabel.foreach(println)
    sc.stop()
  }
}
