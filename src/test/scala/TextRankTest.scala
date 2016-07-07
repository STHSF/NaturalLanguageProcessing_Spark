import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * Created by li on 16/6/23.
  */
object TextRankTest {

  def main(args: Array[String]) {

    val doc = new ListBuffer[(String)]

    val text = Source.fromURL(getClass.getResource(s"/text/${2}.txt")).getLines().mkString("\n")
    text.split(",").foreach(x => doc.+=(x))


    // 构建候选关键词图, 设置窗口大小5
    val textGraph = new ConstructTextGraph("url", 10, doc).constructGraph

    // 输出构建的无向图的边和顶点
    //  textGraph.getEdgeSet.toArray.foreach(println)
    //  textGraph.getNodeSet.toArray.foreach(println)
    //  assert(textGraph.getEdgeSet.size() > 0)
    println((1 to 30).map(i => "=").mkString)

    // 输出提取的关键词
    val keywordExtractor = new PropertyExtractor(textGraph, 5)
    keywordExtractor.extractKeywords(100, 0.85f).foreach(
      node =>
        println(" 关键词: "+node._1," 得分: "+node._2)
    )
    println((1 to 30).map(i => "=").mkString)

    //  获取每个关键词节点的度
    textGraph.getNodeSet.toArray.map(_.asInstanceOf[Node]).foreach {
      node =>
        println (node.getId, node.getDegree)
    }
  }
}
