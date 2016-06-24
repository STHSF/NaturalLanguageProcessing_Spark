import com.kunyandata.nlpsuit.{ConstructTextGraph, KeywordExtractor}
import org.graphstream.graph.Node

import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * Created by li on 16/6/23.
  */
object TextRankTest extends App{

  val doc = new ListBuffer[(String)]
  val text = Source.fromURL(getClass.getResource(s"/text/${2}.txt")).getLines().mkString("\n")
  text.split(",").foreach(x => doc.+=(x))

  // 构建候选关键词图, 设置窗口大小5
  val textGraph = new ConstructTextGraph("test", 30, doc).graph

  // 输出构建的无向图的边和顶点
  //  textGraph.getEdgeSet.toArray.foreach(println)
  //  textGraph.getNodeSet.toArray.foreach(println)
  //  assert(textGraph.getEdgeSet.size() > 0)
  println((1 to 30).map(i => "=").mkString)

  // 输出提取的关键词
  val keywordExtractor = new KeywordExtractor("url",textGraph , 10)
  keywordExtractor.extractKeywords(doc).foreach(println)
  println((1 to 30).map(i => "=").mkString)

  //  获取每个关键词的得分
  textGraph.getNodeSet.toArray.map(_.asInstanceOf[Node]).foreach {
    node =>
      println (node.getId, node.getDegree)
  }
}
