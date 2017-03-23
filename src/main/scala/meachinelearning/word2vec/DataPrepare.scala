package meachinelearning.word2vec

import dataprocess.vipstockstatistic.util.AnsjAnalyzer
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by li on 2016/10/14.
  */
object DataPrepare {

  /**
    * 读文件
    *
    * @param filePath 文本保存的位置
    * @return
    */
  def readData(filePath: String): Array[String] = {

    val doc = Source.fromFile(filePath).getLines().toArray

    doc
  }


  /**
    * 分词
    *
    * @param doc
    * @return
    */
  def docCut(doc: Array[String]): Array[String] = {

    val docSeg = doc.map(x => AnsjAnalyzer.cutNoTag(x)).flatMap(x =>x)

    docSeg
  }


  /**
    * 构建文本向量
    *
    * @param word2vecModel
    * @param docSeg
    * @return
    */
  def docVec(word2vecModel: Word2VecModel, docSeg: Array[String], modelSize: Int): Array[Double] = {

    val docVectors = TextVectors.textVectorsWithModel(docSeg, word2vecModel, modelSize).toArray

    docVectors
  }

  /**
    * 打标签，文本集合构建labeledPoint,集合中文章属于同一类
    *
    * @param label
    * @param docVec
    * @return
    */
  def tagAttacheBatchSingle(label: Double, docVec: RDD[Array[Double]]): RDD[LabeledPoint] = {

    docVec.map{
      row =>
        LabeledPoint(label , Vectors.dense(row))
    }
  }

  /**
    * 打标签，文本集合构建labeledPoint
    *
    * @param docVec
    * @return
    */
  def tagAttacheBatchWhole(docVec: RDD[(Double, Array[Double])]): RDD[LabeledPoint] = {

    docVec.map{
      row =>
        LabeledPoint(row._1 , Vectors.dense(row._2))
    }
  }


  /**
    * 打标签，单篇文本构建labeledPoint
    *
    * @param label
    * @param docVec
    * @return
    */
  def tagAttacheSingle(label: Double, docVec: Array[Double]): LabeledPoint = {

    LabeledPoint(label=1.0 , Vectors.dense(docVec))
  }


  /**
    * 测试代码
    */
  def dataPrepareTest(): Unit ={
    val conf = new SparkConf().setMaster("local").setAppName("DataPrepare")
    val sc = new SparkContext(conf)

    val filePath = "/Users/li/workshop/DataSet/111.txt"
    //    val filePath = "/Users/li/workshop/DataSet/SogouC.reduced/Reduced/C000008/10.txt"

    val word2vecModelPath = "/Users/li/workshop/DataSet/word2vec/result/2016-07-18-15-word2VectorModel"
    val model = Word2VecModel.load(sc, word2vecModelPath)

    val data = readData(filePath)

    val splitData = docCut(data)

    val doVec = docVec(model, splitData, 100)

    val labeledP = tagAttacheSingle(1.0, doVec)
    println(labeledP)


  }


  def main(args: Array[String]) {

    dataPrepareTest()

  }

}
