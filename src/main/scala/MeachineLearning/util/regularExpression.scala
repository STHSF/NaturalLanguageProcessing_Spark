import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

/**
  * Created by li on 16/6/20.
  * 正则表达式,读取文本中所有双引号里面的内容.
  */
object regularExpression extends App{
  val conf = new SparkConf().setMaster("local").setAppName("regularexpression")
  val sc = new SparkContext(conf)

  val data = sc.textFile("file:/Users/li/kunyan/111.txt")



  def quotationMatch(sentence:String): Array[String]={



    val regex = new Regex("\"([^\"]*)\"")
//    val regex = new Regex("(?<=\").{1,}(?=\")")

//      val regex = "\"([^\"]*)\"".r
    val num = regex.findAllIn(sentence)
    val res = new ListBuffer[String]
    while(num.hasNext){
      val item = num.next()
      res += item.replaceAll("\"", "")
    }
      res.toArray
  }

  //  val res = quotationMatch(data)
  data.foreach(

    x =>{
      val res =  quotationMatch(x)
      res.foreach(println)
    }
  )



}
