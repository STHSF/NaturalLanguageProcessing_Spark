package meachinelearning.word2vec


import org.apache.spark.mllib.feature.Word2VecModel

import scala.io.Source

/**
  * Created by li on 2016/10/13.
  * 读取数据构建labelpoint
  */
object SentimentRun {

  case class RawDataRecord(labels: Double ,text: String)

  /**
    * 读取数据
    *
    * @param filePath
    */
  def dataProcess(filePath: String, model: Word2VecModel): Unit = {

    Source.fromFile(filePath).getLines().toArray.map {

      line =>
        val data = line.split(",")
        val vec = textVectors.textVectors(data, model, 100)

        RawDataRecord(data(0).toDouble, vec)
    }
  }

}
