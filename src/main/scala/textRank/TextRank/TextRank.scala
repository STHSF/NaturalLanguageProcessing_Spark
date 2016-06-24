package com.kunyandata.nlpsuit

import org.graphstream.graph.Edge
import org.graphstream.graph.Node
import org.graphstream.graph.implementations.SingleGraph

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by li on 16/6/23.
  */

/**
  * 构建候选关键词图
  *
  * @param graphName 图名称
  * @param winSize 窗口大小
  * @param segWord 分词的结果
  * @return 候选关键词图
  * @author liyu
  */
class ConstructTextGraph(val graphName: String, val winSize: Int, val segWord: ListBuffer[String]) {

  val graph = new SingleGraph(graphName)

  // 获取文本网络的节点
  segWord.foreach(
    word => if (graph.getNode(word) == null) graph.addNode(word)
  )

  // 导入分完词的数据,并通过设置的窗口截取
  var wordSeg = new ListBuffer[(ListBuffer[(String)])]

  val num = segWord.size - winSize
  for (i <- 0 to num) {

    val item = new ListBuffer[(String)]
    for (j <- 0 until winSize) {

      item += segWord(i + j)
    }
    wordSeg += item
  }

  // 获取每个顶点以及所包含的窗口内的邻居节点
  val wordSet = segWord.toSet
  val edgeSet = wordSet.map {
    word => {

      val edgeList = new mutable.HashSet[(String)]
      wordSeg.foreach{
        list => {
          if (list.contains(word)){
            list.foreach(x => edgeList.+=(x))
          }
        }
      }

      (word, edgeList -= word)
    }
  }

  // 构建关键词图的边
  edgeSet.toArray.foreach {
    edge => {
      edge._2.toList.foreach {
        edges =>

          if (graph.getEdge(s"${edge._1}-${edges}") == null &&
            graph.getEdge(s"${edges}-${edge._1}") == null) {
            graph.addEdge(s"${edge._1}-${edges}", edge._1, edges)
            None
          }
      }
    }
  }
}

/**
  * 关键词提取, 输出个文章提取的关键词, 无向图名称为文章的url
  *
  * @param url 文本标识
  * @param num 关键词个数
  * @return 文本的关键词
  * @author liyu
  */
class KeywordExtractor(val url: String, val graph: SingleGraph, val num: Int) {

  def extractKeywords(doc: ListBuffer[(String)]) = {

    val nodes = graph.getNodeSet.toArray.map(_.asInstanceOf[Node])
    val scoreMap = new mutable.HashMap[String, Float]

    // 节点权重初始化
    nodes.foreach(node => scoreMap.put(node.getId, 1.0f))

    // 迭代 迭代传播各节点的权重，直至收敛。
    (1 to 100).foreach {
      i =>
        nodes.foreach {
          node =>
            val edges = node.getEdgeSet.toArray.map(_.asInstanceOf[Edge])
            var score = 1.0f - 0.85f
            edges.foreach {
              edge =>
                val node0 = edge.getNode0.asInstanceOf[Node]
                val node1 = edge.getNode1.asInstanceOf[Node]
                val tempNode = if (node0.getId.equals(node.getId)) node1 else node0
                score += 0.85f * (1.0f * scoreMap(tempNode.getId) / tempNode.getDegree)
            }

            scoreMap.put(node.getId, score)
        }
    }
    // 对节点权重进行倒序排序，从而得到最重要的num个单词，作为候选关键词。
    scoreMap.toList.sortWith(_._2 > _._2).slice(0, num).map(_._1)
  }
}