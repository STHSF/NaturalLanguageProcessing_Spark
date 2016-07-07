

import meachinelearning.textrank.TextRank

import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * Created by li on 16/6/24.
  */
object testRankTest {

  def main(args: Array[String]) {

    val doc = new ListBuffer[(String)]

    val text = Source.fromURL(getClass.getResource(s"/text/${2}.txt")).getLines().mkString("\n")
    text.split(",").foreach(x => doc.+=(x))

    val keyWordList = TextRank.run("url", 5, doc, 3, 100, 0.85f)

    keyWordList.foreach {
      word => {
        println(word._1, word._2)
      }
    }
  }

}
