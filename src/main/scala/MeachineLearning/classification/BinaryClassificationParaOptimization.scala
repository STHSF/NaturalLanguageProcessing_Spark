import java.io.{FileWriter, BufferedWriter, File}

import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ListBuffer, ArrayBuffer}

/**
  * Created by li on 16/4/11.
  * 使用RDD格式读取数据.
  * 对训练集进行分词,去停,卡方降维,最小词频和维度参数寻优
  */
object BinaryClassificationParaOptimization {

  val conf  = new SparkConf().setAppName("test").setMaster("local")
  val sc = new SparkContext(conf)

  /**
    * 计算语料库中的词汇数量,训练集的空间维度
    *
    * @param trainingSet 语料库RDD
    * @return 返回一个整型
    */
  def countWords(trainingSet: RDD[(Double, Array[String])]): Int = {
    val wordCount = trainingSet.flatMap(training => {
      training._2
    }).collect().toSet.size
    wordCount
  }

  /**
    * 去除分词结果中的标点符号和停用词
    *
    * @param content 分词结果
    * @param stopWords 停用词
    * @return 返回一个元素为String的Array
    */
  def removeStopWords(content:Array[String], stopWords:Array[String]): Array[String] ={
    if(content != null){
      var res = content.toBuffer
      stopWords.foreach{
        stopWord =>{
          if(res.contains(stopWord)){
            res = res.filterNot(_ == stopWord) //
          }
        }
      }
      res.toArray
    }else{
      null
    }
  }


  /**
    * 从源数据中产生平衡数据集
    *
    * @param dataRDD  输入数据集
    * @param industry 需要处理的行业
    * @return 返回一个平衡数据集,格式RDD(label, content)
    */
  def dataOperation (dataRDD: RDD[(String)], industry: String, stopWords:Array[String]): RDD[(Double, Array[String])] ={
    // 输入数据格式(label, catagory, content)
    // 获取数据并转化成(label, content)的格式
    val data = dataRDD.map{
      line =>{
        val item = line.split("\t")
        (if (item(1) == industry) "1" else "0", item(2))
      }
    }

    // 平衡数据集
    val key= data.filter(_._1 == "1")
    val size =key.count()
    val per = size * 1.0 / (data.count() - size)
    val result = data.filter(_._1 == "0").randomSplit(Array(per, 1-per), 11l)(0).++(key)

    // 去停用词
    val stopWordRemoved = result.map(
      line =>
        //    val data =line._2.split(" ")// 为什么这样不可以?
        (line._1.toDouble, removeStopWords(line._2.split(","), stopWords))
    )
    stopWordRemoved
  }


  // 正则化

  /**
    *
    * @param trainingRDD 训练集数据
    * @param testRDD 测试集数据
    * @param minDocFreq 文档词频的下限
    * @param numTopFeatures 卡方降维的特征数
    * @return
    */
  def featureSelect (trainingRDD:RDD[(Double, Array[String])],vocabNum: Int, testRDD:RDD[(Double, Array[String])],minDocFreq: Int, numTopFeatures: Int ) :(Int, Int, Double, Double) = {

    /** tf-idf 计算 */
    // 计算tf
    // vocabNum 计算训练集的维度
    //    val vocabNum = countWords(trainingRDD)
    //    println(vocabNum)
    val hashingTFModel = new feature.HashingTF(vocabNum)
    val trainTFRDD = trainingRDD.map(line => {
      val temp = hashingTFModel.transform(line._2)
      (line._1, temp)
    })

    // 计算idf
    val idfModel = new IDF(minDocFreq).fit(trainTFRDD.map(line => {line._2}))
    val labeedTrainTfIdf = trainTFRDD.map( line => {
      val temp = idfModel.transform(line._2)
      LabeledPoint(line._1, temp)
    })

    /** 卡方降维 */
    // 卡方降维特征选择器
    val chiSqSelectorModel = new feature.ChiSqSelector(numTopFeatures).fit(labeedTrainTfIdf)
    val selectedTrain = labeedTrainTfIdf.map(line => {
      val temp = chiSqSelectorModel.transform(line.features)
      LabeledPoint(line.label, temp)
    })

    val byesModel = NaiveBayes.train(selectedTrain, 1.0, "multinomial")

    /** 测试集数据处理 */
    val testTFRDD = testRDD.map{
      line =>
        val item = hashingTFModel.transform(line._2)
        (line._1, item)
    }
    val testTfIdf = testTFRDD.map{
      line =>
        val item = idfModel.transform(line._2)
        LabeledPoint(line._1, item)
    }
    val selectedTest = testTfIdf.map{
      line =>
        val item = chiSqSelectorModel.transform(line.features)
        LabeledPoint(line.label, item)
    }

    /** 预测 */
    val predictionAndLabels = selectedTest.map {
      line =>
        val prediction = byesModel.predict(line.features)
        //      println((prediction, line.label))
        (prediction, line.label)
    }

    /** 模型评价 */
    val metrics = new MulticlassMetrics(predictionAndLabels)
    //    println("Confusion matrix:")
    //    println(metrics.confusionMatrix)
    val labels = metrics.labels
    labels.foreach { l =>
      // Precision by label
      println(s"Precision($l) = " + metrics.precision(l))
      // Recall by label
      println(s"Recall($l) = " + metrics.recall(l))
      // False positive rate by label
      println(s"FPR($l) = " + metrics.falsePositiveRate(l))
      // F-measure by label
      println(s"F1-Score($l) = " + metrics.fMeasure(l))
    }
    (minDocFreq, numTopFeatures, metrics.precision(1.0), metrics.recall(1.0))

  }

  /**
    * 参数寻优 最小频数,最大特征数
    *
    * @param trainingRDD 训练集
    * @param testRDD 测试集
    * @return
    */
  def crossValidation(trainingRDD:RDD[(Double, Array[String])],testRDD:RDD[(Double, Array[String])] ): Array[(Int, Int, Double, Double) ]= {
    val res = new ArrayBuffer[(Int, Int, Double, Double)]
    val vocabNum = countWords(trainingRDD)
    // 交叉验证选择的最低和特征维度数目
    // 文档中词出现的最少次数
    val minDocFreqArray = Array(1, 20, 50 ,100)
    // 卡方选择的特征维度
    val numTopFeaturesArray = Array(1000, 2000, 3000, 5000,vocabNum/10, vocabNum/5, vocabNum/2)

    // 数据集10份 计算结果取平均
    minDocFreqArray.foreach{
      minDocFreq =>{
        numTopFeaturesArray.foreach{
          numTopFeatures =>{
            // 模型
            val result = featureSelect (trainingRDD, vocabNum,testRDD ,minDocFreq, numTopFeatures)
            // 写入文件
            res.+=:(result)
          }
        }
      }
    }
    res.toArray
  }

  def main(args:Array[String]) {

    //    val minDocFreq = 1
    //    val numTopFeatures = 2000
    val setTextPath = "/Users/li/kunyan/DataSet/trainingsetUnbalance/YSJS.txt"
    val setStopWordPath = "/Users/li/kunyan/DataSet/stop_words_CN"
    val industury = "非银金融"
    val dataFile = "/Users/li/kunyan/DataSet/201603.txt"


    /** 数据导入 */
    val resRDD = sc.textFile(setTextPath).cache()

    /** 停用词导入 */
    val stopWords = sc.textFile(setStopWordPath).collect()
    val listbuffer = new ListBuffer[(Int, Int, Double, Double)]


    /** 构造平衡集,并去停*/
    val stopWordRemoved = dataOperation(resRDD, industury, stopWords)
    //70%作为训练数据，30%作为测试数据
    val splits = stopWordRemoved.randomSplit(Array(0.9, 0.1), seed = 11L)
    val trainingRDD = splits(0)
    val testRDD = splits(1)
    val result = crossValidation(trainingRDD, testRDD)
    // result.foreach(println)
    // 平均值

    // 结果保存到文件中
    val DataFile = new File(dataFile)
    val bufferWriter = new BufferedWriter(new FileWriter(DataFile))
    for(item <- result) {
      //      val cata = map.get(item._1).get
      bufferWriter.write( item._1 + "\t" + item._2 + "\t"+ item._3 + "\t" +item._4 +"\n")
    }
    bufferWriter.flush()
    bufferWriter.close()
    sc.stop()
  }
}
