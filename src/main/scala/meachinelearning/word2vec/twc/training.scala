package meachinelearning.word2vec.twc

import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhangxin on 16-11-9.
  *
  * 分类模型训练
  */
object training {


  def training(): Unit ={

    val conf = new SparkConf().setAppName("W2V").setMaster("local")
    val sc = new SparkContext(conf)
//    val jsonPath = "/home/zhangxin/work/workplace_scala/Sentiment/src/main/scala/classificationW2V/W2VJsonConf.json"
    val jsonPath = "/Users/li/workshop/NaturalLanguageProcessing/src/main/scala/meachinelearning/word2vec/twc/W2VJsonConf.json"

    // 非平衡集
//    val docsPath = "/home/zhangxin/work/workplace_scala/Data/trainingsetUnbalance/JSJ.txt"
//    val docs = sc.textFile(docsPath).collect()

    // 平衡集
//    val docsPath = "/home/zhangxin/work/workplace_scala/Data/trainingSets/房地产"
//    val docsPath = "/home/zhangxin/work/workplace_scala/Data/trainingSets/有色金属"
//    val docsPath = "/home/zhangxin/work/workplace_scala/Data/trainingSets/保险"
//    val docsPath = "/home/zhangxin/work/workplace_scala/Data/trainingSets/医药"
//    val docsPath = "/home/zhangxin/work/workplace_scala/Data/trainingSets/计算机"
    val docsPath = "/Users/li/workshop/DataSet/trainingSets/工程建筑"

    val docs = sc.textFile(docsPath).collect()

    // inputs
    val data = processing.process_weight(docs, sc, jsonPath)
    println("[完成DOC2Vec模型]>>>>>>>>>>>>>>>>>")

    val dataRdd = sc.parallelize(data)
    val splits = dataRdd.randomSplit(Array(0.8, 0.2), seed = 11L)
    val train = splits(0)
    val test = splits(1)

    val model = SVMWithSGD.train(train, 1000)
//    val model = LogisticRegressionWithSGD.train(train, 5000)
    println("[完成模型训练]>>>>>>>>>>>>>>>>>>>")


    val predictAndLabels = test.map{
      case LabeledPoint(label, features) =>
        val prediction = model.predict(features)
        (prediction, label)
    }

    val metrics = new MulticlassMetrics(predictAndLabels)
    println(s"[综合_Precison] ${metrics.precision}")
    println(s"[Labels] ${metrics.labels.toList}")
    metrics.labels.foreach(label => {
      println(s"[${label}_Precison] ${metrics.precision(label)}")
    })

  }

  def main(args: Array[String]): Unit = {
    training()
  }

}
