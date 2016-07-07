package ml.classification

import java.io._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.feature.PCA
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by li on 16/4/11.
  */


object TrainingProcessWithPCA {

  /**
    * 基于RDD的训练过程，其中包括了序列化tf，idf，chisqselector，nbmodel 4个模型。
    *
    * @param train 训练集
    * @param test 测试集
    * @param parasDoc idf最小文档频数参数
    * @param parasFeatrues 特征选择数量参数
    * @return 返回（精度，召回率）
    */
  def trainingProcessWithRDD(train: RDD[(Double, Array[String])], test: RDD[(Double, Array[String])], parasDoc: Int, parasFeatrues: Int, writeModel:Boolean) = {
    // 构建hashingTF模型，同时将数据转化为LabeledPoint类型
    val vocabNum = countWords(train)
    val hashingTFModel = new feature.HashingTF(vocabNum)
    val trainTFRDD = train.map(line => {
      val temp = hashingTFModel.transform(line._2)
      (line._1, temp)
    })

    // 计算idf
    val idfModel = new feature.IDF(parasDoc).fit(trainTFRDD.map(line => {line._2}))
    val labeedTrainTfIdf = trainTFRDD.map( line => {
      val temp = idfModel.transform(line._2)
      LabeledPoint(line._1, temp)
    })

    // PCA降维
    val pcaModel = new PCA(parasFeatrues).fit(labeedTrainTfIdf.map(_.features))

    val trainProjected = labeedTrainTfIdf.map(p =>p.copy(features = pcaModel.transform(p.features)))


//    val chiSqSelectorModel = new feature.ChiSqSelector(parasFeatrues).fit(labeedTrainTfIdf)
//    val selectedTrain = labeedTrainTfIdf.map(line => {
//      val temp = chiSqSelectorModel.transform(line.features)
//      LabeledPoint(line.label, temp)
//    })

    println("+++++++++++++++++++++++++++++++++++++++++++++特征选择结束++++++++++++++++++++++++++++++++++++++++++++++++++")

    // 创建贝叶斯分类器
    val nbModel = NaiveBayes.train(trainProjected, 1.0, "multinomial")

    if(writeModel){
      val tfModelOutput = new ObjectOutputStream(new FileOutputStream("D:/tfModel"))
      tfModelOutput.writeObject(hashingTFModel)
      val idfModelOutput = new ObjectOutputStream(new FileOutputStream("D:/idfModel"))
      idfModelOutput.writeObject(idfModel)
      val chiSqSelectorModelOutput = new ObjectOutputStream(new FileOutputStream("D:/chiSqSelectorModel"))
      chiSqSelectorModelOutput.writeObject(pcaModel)
      val nbModelOutput = new ObjectOutputStream(new FileOutputStream("D:/nbModel"))
      nbModelOutput.writeObject(nbModel)
    }

    // 根据训练集同步测试集特征
    val testRDD = test.map(line => {
      val temp = line._2
      val tempTf = hashingTFModel.transform(temp)
      val tempTfidf = idfModel.transform(tempTf)
      LabeledPoint(line._1, tempTfidf)
    })

    val testProjected = testRDD.map(p =>p.copy(features = pcaModel.transform(p.features)))

    // test
    val predictionAndLabels = testProjected.map {line =>
      val prediction = nbModel.predict(line.features)
      //      println((prediction, line.label))
      (prediction, line.label)
    }

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

  /**
    * 网格参数寻优，并输出成文本
    *
    * @param df 输入的训练集，为交叉验证准备的5份训练测试数据
    * @param parasDoc idf最小文档频数参数的序列
    * @param parasFeatrues 特征选择数量参数的序列
    */
  def tuneParas(df: Array[Map[String, RDD[(Double, Array[String])]]], parasDoc:Array[Int], parasFeatrues:Array[Int], indusName: String) = {
    val hdfsConf = new Configuration()
    hdfsConf.set("fs.defaultFS", "hdfs://222.73.34.92:9000")
    val fs = FileSystem.get(hdfsConf)
    val output = fs.create(new Path("/mlearning/ParasTuningResult"))
    val writer = new PrintWriter(output)
    var result:Map[String,(Double, Double)] = Map()
    parasDoc.foreach(paraDoc => {
      parasFeatrues.foreach(paraFeatrues => {
        df.foreach(data => {
          val paraSets = paraDoc.toString + "_" + paraFeatrues.toString
          val results = trainingProcessWithRDD(data("train"), data("test"), paraDoc, paraFeatrues, writeModel = false)
          result += (paraSets -> results)
          val writeOut = indusName + "\t" +  paraSets + "\t\tPrecision:" + results._1 + "\tRecall:" + results._2 + "\n"
          writer.write(writeOut)
        })
        writer.write("\n")
      })
      writer.write("\n\n")
    })
    result.foreach(println)
    writer.close()
  }

  /**
    * 从本地保存的文章对应行业文本数据变形为（行业，文章url数组）
    *
    * @param args 一个Array，每一个元素是一个tuple，包含文章url和对应的行业
    * @return 返回一个map，key为行业，value为url
    */
  private def getIndusTextUrl(args: Array[(String, String)]): Map[String, ArrayBuffer[String]] = {

    val result = mutable.Map[String, ArrayBuffer[String]]()
    args.foreach(line =>{
      val industries = line._2.split(",")
      industries.foreach(indus => {
        if (result.keys.toArray.contains(indus)){
          result(indus).append(line._1)
        }else{
          result.+=((indus, ArrayBuffer[String]()))
          result(indus).append(line._1)
        }
      })
    })
    result.toMap
  }

  /**
    * 匹配数据集合中对应的url的数据
    *
    * @param targetIDList 目标id（一般的，用文章的url作为文章的唯一标识）
    * @param dataSet 所有文章的数据集，每一个元素为（url, 正文）
    * @return 返回一个数组，元素是RDD[(Double, Array[String])]
    */
  private def matchRDD(targetIDList: Array[String], dataSet:RDD[(String, Array[String])]): RDD[(Double, Array[String])] ={

    //    val intersectId = idA.toSet & idB.toSet
    val poInst = dataSet.map(line => {
      if (targetIDList.contains(line._1)) (1.0, line._2)
    }).filter(_ != ()).map(_.asInstanceOf[(Double, Array[String])])
    val poInstNum = poInst.count()

    val neInst = dataSet.map(line => {
      if (!targetIDList.contains(line._1)) (0.0, line._2)
    }).filter(_ != ()).map(_.asInstanceOf[(Double, Array[String])])
    val neInstNum = neInst.count()

    if (poInstNum <= neInstNum){
      poInst.++(neInst.sample(withReplacement = false, fraction = poInstNum*1.0/neInstNum, seed = 2016))
    }else{
      poInst.++(neInst.sample(withReplacement = true, fraction = poInstNum*1.0/neInstNum, seed = 2016))
    }
  }

  /**
    * 计算语料库中的词汇数量
    *
    * @param trainingSet 语料库RDD
    * @return 返回一个整形
    */
  def countWords(trainingSet: RDD[(Double, Array[String])]): Int = {
    val wordCount = trainingSet.flatMap(training => {
      training._2
    }).collect().toSet.size
    wordCount
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("MlTraining")
    val sc = new SparkContext(conf)

    //    val text = Source.fromFile("D:/mlearning/trainingLabel.new").getLines().toArray.map(line => {
    //      val temp = line.split("\t")
    //      (temp(0), temp(1))
    //    })
    //
    //    val writer = new PrintWriter(new File("D:/labeledContent"))
    //    val a = getIndusTextUrl(text)
    //    a.foreach(line => {
    //      writer.write(line._1 + "\t" + line._2.mkString(",") + "\n")
    //    })
    //    writer.close()

    val labeledContent = sc.textFile("hdfs://222.73.34.92:9000/mlearning/trainingData/labeledContent").collect().map(line => {
      val temp = line.split("\t")
      (temp(0), temp(1).split(","))
    }).toMap

    val totalTrianRDD = sc.textFile("hdfs://222.73.34.92:9000/mlearning/trainingData/segTrainSet").repartition(4).map(line => {
      val temp = line.split("\t")
      if (temp.length == 2) {
        (temp(0), temp(1).split(","))
      }
    }).filter(_ != ()).map(_.asInstanceOf[(String, Array[String])])

    //    val writer = new PrintWriter(new File("D:/trainingStatic"))
    //    labeledContent.foreach(line => {
    //      val temp = matchRDD(line._2, totalTrianRDD)
    //      val posTemp = temp.filter(_._1 == 1.0).count()
    //      writer.write("训练集\'" + line._1 + "\'的数量为" + ":" + temp.count() + "\t" + "其中正例的样本数量为" + posTemp + "\n")
    //      writer.flush()
    //    })
    //    writer.close()

    val trainingSets = labeledContent.map(target => {
      val tempRDD = matchRDD(target._2, totalTrianRDD).repartition(4).randomSplit(Array(0.2, 0.2, 0.2, 0.2, 0.2), seed = 2016L)
      val dataSet = Array(
        Map("train" -> tempRDD(0).++(tempRDD(1)).++(tempRDD(2)).++(tempRDD(3)), "test" -> tempRDD(4)),
        Map("train" -> tempRDD(0).++(tempRDD(1)).++(tempRDD(2)).++(tempRDD(4)), "test" -> tempRDD(3)),
        Map("train" -> tempRDD(0).++(tempRDD(1)).++(tempRDD(3)).++(tempRDD(4)), "test" -> tempRDD(2)),
        Map("train" -> tempRDD(0).++(tempRDD(2)).++(tempRDD(3)).++(tempRDD(4)), "test" -> tempRDD(1)),
        Map("train" -> tempRDD(1).++(tempRDD(2)).++(tempRDD(3)).++(tempRDD(4)), "test" -> tempRDD(0))
      )
      (target._1, dataSet)
    })

    trainingSets.foreach(trainRDD => {
      TrainingProcessWithPCA.tuneParas(trainRDD._2, Array(1, 2), Array(100, 300, 500), trainRDD._1)
      println("行业\'" + trainRDD._1 + "\'参数调优已完成")
    })
    sc.stop()
  }



  //1.5.2的机器学习库中没有实现chisqselector的pipline，所以参数寻优在RDD上完成
  //  /**
  //    * 基于dataframe的训练，主要用于网格参数寻优。
  // *
  //    * @param sc sparkcontext
  //    * @param train 训练集
  //    * @param test 测试集
  //    * @param parasDoc idf最小文档频数参数
  //    * @param parasFeatrues 特征选择数量参数
  //    * @return 返回（精度，召回率）
  //    */
  //  def trainingProcessWithDF(sc: SparkContext, train:RDD[Seq[Object]], test: RDD[Seq[Object]], parasDoc: Int, parasFeatrues: Int) = {
  //    val sqlContext = new SQLContext(sc)
  //    val schema =
  //      StructType(
  //        StructField("id", StringType, nullable = false) ::
  //          StructField("category", StringType, nullable = false) ::
  //          StructField("content", ArrayType(StringType, containsNull = true), nullable = false) ::
  //          StructField("label", DoubleType, nullable = false) :: Nil)
  //    val dataDF = sqlContext.createDataFrame(train.map(line => {
  //      Row(line(0), line(1), line(2).asInstanceOf[Seq[String]].toArray, if(line(1) == "881155") 1.0 else 0.0)
  //    }), schema).toDF()
  //
  //    val testDF = sqlContext.createDataFrame(test.map(line => {
  //      Row(line(0), line(1), line(2).asInstanceOf[Seq[String]].toArray, if(line(1) == "881155") 1.0 else 0.0)
  //    }), schema).toDF()
  //
  //    // 去除停用词
  ////    val stopWordsRemover = new StopWordsRemover()
  ////      .setStopWords(stopWords)
  ////      .setInputCol("content")
  ////      .setOutputCol("filtered")
  //    //  val worddf = stopWordsRemover.transform(wordDataFrame)
  //
  ////     构建向量空间模型
  //    val hashingTFModel = new HashingTF()
  //      .setInputCol("content")
  //      .setOutputCol("rawFeatures")
  //      .setNumFeatures(55000)
  //
  ////    val cvModel = new CountVectorizer()
  ////      .setInputCol(stopWordsRemover.getOutputCol)
  ////      .setOutputCol("rawFeatures")
  //
  //    // 计算idf值，并根据向量空间模型中的tf值获得tfidf
  //    val idfModel = new IDF()
  //      .setInputCol(hashingTFModel.getOutputCol)
  //      .setOutputCol("features")
  //      .setMinDocFreq(parasDoc)
  //
  //    //  val inppput = new ObjectInputStream(new FileInputStream("D:/idfModel"))
  //    //  val idfModel = inppput.readObject().asInstanceOf[IDF]
  //
  //    val featureSelector = new ChiSqSelector()
  //      .setNumTopFeatures(parasFeatrues)
  //      .setFeaturesCol(idfModel.getOutputCol)
  //      .setLabelCol("label")
  //      .setOutputCol("selectedFeatures")
  //
  //    val vectorSpacePipline = new Pipeline()
  //      .setStages(Array(hashingTFModel, idfModel, featureSelector))
  //    val vectorSpacePiplineM = vectorSpacePipline.fit(dataDF)
  //    val trainCM = vectorSpacePiplineM.transform(dataDF)
  //    val testCM = vectorSpacePiplineM.transform(testDF)
  //    trainCM.show
  //
  //
  //    // 转换数据类型
  //    val trainData = trainCM.select("label", "selectedFeatures").map(line => {
  //      LabeledPoint(line.getDouble(0), line.getAs[SparseVector](1))
  //    })
  //
  //    val testData = testCM.select("label", "selectedFeatures").map(line => {
  //      LabeledPoint(line.getDouble(0), line.getAs[SparseVector](1))
  //    })
  //
  //    val nbModel = NaiveBayes.train(trainData, 1.0, "multinomial")
  //
  //    val predictionAndLabels = testData.map {line =>
  //      val prediction = nbModel.predict(line.features)
  //      (prediction, line.label)
  //    }
  //
  //    val metrics = new MulticlassMetrics(predictionAndLabels)
  //    println("Confusion matrix:")
  //    println(metrics.confusionMatrix)
  //
  //    // Precision by label
  //    val labels = metrics.labels
  //    labels.foreach { l =>
  //      println(s"Precision($l) = " + metrics.precision(l))
  //    }
  //
  //    // Recall by label
  //    labels.foreach { l =>
  //      println(s"Recall($l) = " + metrics.recall(l))
  //    }
  //
  //    // False positive rate by label
  //    labels.foreach { l =>
  //      println(s"FPR($l) = " + metrics.falsePositiveRate(l))
  //    }
  //
  //    // F-measure by label
  //    labels.foreach { l =>
  //      println(s"F1-Score($l) = " + metrics.fMeasure(l))
  //    }
  //    (metrics.precision(1.0), metrics.recall(1.0))
  //  }

  //  val conf = new SparkConf().setAppName("mltest").setMaster("local")
  //  val sc = new SparkContext(conf)
  //
  //  val data = Source.fromFile("D:\\WorkSpace\\Spark_WorkSpace\\ein" +
  //    "\\text_classification\\1.1\\Spark_NLP_suit\\src\\main" +
  //    "\\resources\\train\\wordseg_881155").getLines().toArray
  //
  //  // 获取停用词
  //  val stopWords = Source.fromFile("D:\\WorkSpace\\Spark_WorkSpace" +
  //    "\\ein\\text_classification\\1.1\\Spark_NLP_suit\\src\\main" +
  //    "\\resources\\dicts\\stop_words_CN").getLines().toArray
  //
  //  // 基于RDD的模型训练流程
  //  val dataRDD = sc.parallelize(data.map(line => {
  //    val temp = line.split("\t")
  //    val removedStopWords = WordSeg.removeStopWords(temp(2).split(" "), stopWords)
  //    Seq(temp(0), temp(1), removedStopWords.toSeq)
  //  }))
  //
  //  val dataSets = dataRDD.randomSplit(Array(0.2, 0.2, 0.2, 0.2, 0.2), seed = 2016L)
  //  val dataSet = Seq(
  //    Map("train" -> dataSets(0).++(dataSets(1)).++(dataSets(2)).++(dataSets(3)), "test" -> dataSets(4)),
  //    Map("train" -> dataSets(0).++(dataSets(1)).++(dataSets(2)).++(dataSets(4)), "test" -> dataSets(3)),
  //    Map("train" -> dataSets(0).++(dataSets(1)).++(dataSets(3)).++(dataSets(4)), "test" -> dataSets(2)),
  //    Map("train" -> dataSets(0).++(dataSets(2)).++(dataSets(3)).++(dataSets(4)), "test" -> dataSets(1)),
  //    Map("train" -> dataSets(1).++(dataSets(2)).++(dataSets(3)).++(dataSets(4)), "test" -> dataSets(0))
  //  )
  //  tuneParas(dataSet, Array(1,2),
  //    Array(500, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000,
  //      5500, 6000, 6500, 7000, 7500, 8000, 8500, 9000, 9500, 10000))

  //  val result1 = trainingProcessWithDF(sc, dataSet(0)("train"), dataSet(0)("test"), 0, 500)
  //  val result2 = trainingProcessWithRDD(dataSet(0)("train"), dataSet(0)("test"), 0, 500)
  //  println(result1)
  //  println(result2)
  //  trainingProcessWithDF(sc, dataSet(0)("train"), dataSet(0)("test"), 2, 500)
  //  trainingProcessWithRDD(trainDataRDD, testDataRDD)

  //  sc.stop()

}
