package meachinelearning.word2vec.twc

import breeze.linalg.Vector
import com.kunyandata.nlpsuit.util.{JsonConfig, KunyanConf, TextPreprocessing}
import com.kunyandata.nlpsuit.wordExtraction.TextRank
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by zhangxin on 16-11-9.
  *
  * 数据处理:利用word2vec模型处理输入数据,输出为labelpoint格式
  */
object processing {

  // 构建分类停用词
  val commonWords = mutable.Map[String, Int]()

  //已分词文档,非平衡集, 考虑关键词权重
  def process_weight(docs:Array[String], sc: SparkContext, jsonPath:String): Array[(LabeledPoint)] = {
    val docsTemp = docs.map(doc  => {
      val temp = doc.split("\t")
      val label = temp(0)
      val seg = temp(1).split(",")

      //textRank
      val keywords = TextRank.run("k", 10, seg.toList, 20, 50, 0.85f)
      val keywordsFilter = keywords.toArray.filter(word => word._1.length >= 2)

      println(s"[${label}] "+keywordsFilter.toList)

      (label, keywordsFilter)
    })

    println(s"[1.0] ${docsTemp.map(_._1.equals("1.0")).length}")
    println(s"[0.0] ${docsTemp.map(_._1.equals("0.0")).length}")

    val jsonconf = new JsonConfig
    jsonconf.initConfig(jsonPath)

    val modelPath = jsonconf.getValue("w2v", "w2vmodelPath")
    val modelSize = jsonconf.getValue("w2v", "w2vmodelSize").toInt
    val isModel = jsonconf.getValue("w2v", "isModel").toBoolean

    if (isModel) {

      val model = Word2VecModel.load(sc, modelPath)

      val result = docsTemp.map(doc => {
        val resultTemp = doc2vecWithModel_weight(doc._2, model, modelSize)
        val vector = Vectors.dense(resultTemp)
        val label = doc._1.toDouble
        LabeledPoint(label, vector)
      })

      return result
    }else{
      return null
    }

  }

  //已分词文档,非平衡集
  def process(docs:Array[String], sc: SparkContext, jsonPath:String): Array[(LabeledPoint)] = {
    val docsTemp = docs.map(doc  => {
      val temp = doc.split("\t")
      val label = temp(0)
      val seg = temp(1).split(",")

      //textRank
      val keywords = TextRank.run("k", 10, seg.toList, 30, 50, 0.85f)
      val keywordsFilter = keywords.map(_._1).toArray.filter(word => word.length >= 2)

//      val keywordsFilter = keywords.map(_._1).toArray.filter(word => word.length >= 2)

//      keywordsFilter.map(word => {
//        if(commonWords.keySet.contains(word)){
//          commonWords(word) += 1
//        }else{
//          commonWords +=(word -> 1)
//        }
//      })

      println(s"[${label}] "+keywordsFilter.toList)

      (label, keywordsFilter)
    })

    println(s"[1.0] ${docsTemp.map(_._1.equals("1.0")).length}")
    println(s"[0.0] ${docsTemp.map(_._1.equals("0.0")).length}")

    val jsonconf = new JsonConfig
    jsonconf.initConfig(jsonPath)

    val modelPath = jsonconf.getValue("w2v", "w2vmodelPath")
    val modelSize = jsonconf.getValue("w2v", "w2vmodelSize").toInt
    val isModel = jsonconf.getValue("w2v", "isModel").toBoolean

    if (isModel) {

      val model = Word2VecModel.load(sc, modelPath)

      val result = docsTemp.map(doc => {
        val resultTemp = doc2vecWithModel(doc._2, model, modelSize)
        val vector = Vectors.dense(resultTemp)
        val label = doc._1.toDouble
        LabeledPoint(label, vector)
      })

      return result
    }else{
      val model = new mutable.HashMap[String, Array[Double]]()
      sc.textFile(modelPath).map(line => {
        val temp = line.split("\t")
        model.put(temp(0), temp(1).split(",").map(_.toDouble))
      }).collect()

      val result = docsTemp.map(doc => {
        val resultTemp = doc2vecWithHash(doc._2, model, modelSize)
        val vector = Vectors.dense(resultTemp)
        val label = doc._1.toDouble
        LabeledPoint(label, vector)
      })

      return result
    }
  }

  //未分词, (标签, 全文)
  def process(docs:Array[(String, String)], sc: SparkContext, jsonPath:String): Array[(LabeledPoint)] = {

    val jsonconf = new JsonConfig
    jsonconf.initConfig(jsonPath)

    val kunyan = new KunyanConf
    kunyan.set(jsonconf.getValue("kunyan","ip"),jsonconf.getValue("kunyan","port").toInt)

    val stopWords = sc.textFile(jsonconf.getValue("kunyan", "stopwords")).collect()

    val docsCut = docs.map(doc => {
      val docCut = TextPreprocessing.process(doc._2, stopWords, kunyan)
      (doc._1, docCut)
    })

    val modelPath = jsonconf.getValue("w2v", "w2vmodelPath")
    val modelSize = jsonconf.getValue("w2v", "w2vmodelSize").toInt
    val isModel = jsonconf.getValue("w2v", "isModel").toBoolean

    if (isModel) {

      val model = Word2VecModel.load(sc, modelPath)

      val result = docsCut.map(doc => {
        val resultTemp = doc2vecWithModel(doc._2, model, modelSize)
        val vector = Vectors.dense(resultTemp)
        val label = getLabel(doc._1)
        LabeledPoint(label, vector)
      })

      return result
    }else{
      val model = new mutable.HashMap[String, Array[Double]]()
      sc.textFile(modelPath).map(line => {
        val temp = line.split("\t")
        model.put(temp(0), temp(1).split(",").map(_.toDouble))
      }).collect()

      val result = docsCut.map(doc => {
        val resultTemp = doc2vecWithHash(doc._2, model, modelSize)
        val vector = Vectors.dense(resultTemp)
        val label = getLabel(doc._1)
        LabeledPoint(label, vector)
      })

      return result
    }
  }

  //获得标签编号
  private def getLabel(label: String): Double = {
    label match {
      case "neg" => return 1.0
      case "neg" => return 1.0
      case "neg" => return 1.0
      case "neg" => return 1.0
    }
  }

  // 基于word2vec model 获取单文档词向量, 考虑关键词权重
  private def doc2vecWithModel_weight(doc: Array[(String, Float)], model:Word2VecModel, modelSize: Int): Array[Double] = {

    var resultTemp = new Array[Double](modelSize)
    var wordTemp = new Array[Double](modelSize)

    doc.foreach(word => {
      try {
        wordTemp = model.transform(word._1).toArray
      }
      catch {
        case e: Exception => wordTemp = Vector.zeros[Double](modelSize).toArray
      }

      for (i <- resultTemp.indices){
        resultTemp(i) += wordTemp(i) * word._2
      }
    })

    val docVec = resultTemp
    docVec
  }

  // 基于word2vec model 获取单文档词向量
  private def doc2vecWithModel(doc: Array[String], model:Word2VecModel, modelSize: Int): Array[Double] = {

    var resultTemp = new Array[Double](modelSize)
    var wordTemp = new Array[Double](modelSize)

    doc.foreach(word => {
      try {
        wordTemp = model.transform(word).toArray
      }
      catch {
        case e: Exception => wordTemp = Vector.zeros[Double](modelSize).toArray
      }

      for (i <- resultTemp.indices){
        resultTemp(i) += wordTemp(i)
      }
    })

    val docVec = resultTemp.map(vec => vec/doc.length)
    docVec
  }

  // 基于txt型文件获取单文档的词向量
  private def doc2vecWithHash(doc: Array[String], model:mutable.HashMap[String, Array[Double]], modelSize: Int) = {
    var resultTemp = new Array[Double](modelSize)
    var wordTemp = new Array[Double](modelSize)

    doc.foreach(word => {
      try {
        wordTemp = model(word)
      }
      catch {
        case e: Exception => wordTemp = Vector.zeros[Double](modelSize).toArray
      }

      for (i <- resultTemp.indices){
        resultTemp(i) += wordTemp(i)
      }
    })

    val docVec = resultTemp.map(vec => vec/doc.length)
    docVec
  }

  //测试
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("W2V")
    val sc = new SparkContext(conf)

    val w2vPath = "hdfs://61.147.114.85:9000/home/word2vec/model-10-100-20/2016-08-16-word2VectorModel"

    val jsonPath = ""
    val doc = Array("我","爱","中国", "共产党")
    val result = process(doc, sc, jsonPath)
    print(result.toList)
  }

}
