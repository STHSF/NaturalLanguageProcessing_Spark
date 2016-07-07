package util

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * Created by li on 16/4/6.
  * 所输入的数据中有的一个url会对应多个catagory,将具有相同URL的catagory单独分隔开,变成一一对应的值
  */
object UrlCategoryTrim extends App {

  val conf = new SparkConf().setAppName("urlCatagoryTrim").setMaster("local")
  val sc = new SparkContext(conf)


  val data = Source.fromFile("/Users/li/Downloads/trainingLabel(0).new").getLines().toArray.map{
    line =>
      val tmp = line.split("\t")
      (tmp(1), tmp(0))

  }



  // 判断如果catagory中有多个的将其分开并与url对应
  def splitCategory(tuple:(String,String)):ListBuffer[(String)] ={
    val listBuffer = new ListBuffer[(String)]
    val cata = tuple._1.split(",")
    if(cata.length < 1){
      listBuffer.+=(tuple._2 + "\t" + tuple._1)
    }else{
      for(item <- cata){
        listBuffer.+=(tuple._2+ "\t" +item)
      }
    }
    listBuffer
  }

  //  data.flatMap(splitCatagory).foreach(println)
  // 保存到文件中
  val dataFile = new File("/users/li/Downloads/trainglabel3.txt")
  val fileWriter = new FileWriter(dataFile)
  val bufferWriter = new BufferedWriter(fileWriter)

  data.flatMap(splitCategory).foreach(
    line =>
      bufferWriter.write(line + "\n")
  )
  bufferWriter.flush()
  bufferWriter.close()

}


