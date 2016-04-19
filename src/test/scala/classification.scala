import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by li on 16/3/31.
  */
object classification {

  val conf = new SparkConf().setAppName("classification").setMaster("local")
  val sc = new SparkContext(conf)


  def getFile(url: String): RDD[(String, String)] ={
    val content  = sc.textFile(url).map{
      line =>
        val data  =  line.split("\t")
        if (data.length > 1) data(0) -> data(1)
    }.filter( _ != ()).map(_.asInstanceOf[(String, String)])
    content
  }


  def getTrainingset(catagory: RDD[(String, String)], content: RDD[(String, String)], label: String, dataFile: String): Unit  ={
    //    val trainingSet = new ArrayBuffer[String ]
    val DataFile = new File(dataFile)
    val bufferWriter = new BufferedWriter(new FileWriter(DataFile))
    content.map {
      line =>
        catagory.map{
          data =>
            bufferWriter.write((if(label == line._1) "1" else "0") + "\t" + line._1 + "\t"+ line._2 + "\n")
          // val trainingdata = (if(catagory == line._1) "1" else "0") + "\t" + line._1 + "\t"+ line._2
          // trainingSet += trainingdata
        }
    }
    bufferWriter.flush()
    bufferWriter.close()

  }



  //    val DataFile = new File(dataFile)
  //    val bufferWriter = new BufferedWriter(new FileWriter(DataFile))
  //    for(item <- list) {
  //      val cata = map.get(item._1).get
  //      bufferWriter.write((if(cata == catagory) "1" else "0") + "\t" + cata + "\t"+ item._2 + "\n")
  //    }
  //    bufferWriter.flush()
  //    bufferWriter.close()
  //  }

  def main(args: Array[String]) {

    //    val urlContent = new collection.mutable.HashMap[String , String ]
    //    val urlCatagory = new ListBuffer[(String, String)]
    val catagory1 = "有色金属"
    val datafile1 = "/users/li/Downloads/2222.txt"

    val url1 = "/users/li/Downloads/segTraining"
    val url2 = "/users/li/Downloads/traininglabel"

    val urlContent = getFile(url1)
    val urlCatagory = getFile(url2)

    val res = getTrainingset(urlCatagory, urlContent, catagory1, datafile1)

  }





}
