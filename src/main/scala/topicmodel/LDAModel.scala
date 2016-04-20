package com.kunyandata.nlpsuit.topicmodel

/**
  * Created by li on 2016/4/28.
  */

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.mllib.clustering.LDA
//import org.apache.spark.rdd.RDD

object LDAModel extends App{

  val conf = new SparkConf().setAppName("TopicModel").setMaster("local")
  val sc = new SparkContext(conf)

  // Load documents from text files, 1 document per file
  val corpus: RDD[String] = sc.wholeTextFiles("/Users/li/kunyan/docs/*.md").map(_._2)

  // Split each document into a sequence of terms (words)
  val tokenized: RDD[Array[String]] =
    corpus.map(_.toLowerCase.split("\\s")).map(_.filter(_.length > 3).filter(_.forall(java.lang.Character.isLetter)))

  tokenized.foreach(println)
  // Choose the vocabulary.
  //   termCounts: Sorted list of (term, termCount) pairs
  val termCounts: Array[(String, Long)] =
    tokenized.flatMap(_.map(_ -> 1L)).reduceByKey(_ + _).collect().sortBy(-_._2)

  termCounts.foreach(println)

  // vocabArray: Chosen vocab (removing common terms)
  val numStopwords = 20
  val vocabArray: Array[String] =
    termCounts.takeRight(termCounts.length - numStopwords).map(_._1)

  // vocab: Map term -> term index
  val vocab: Map[String, Int] = vocabArray.zipWithIndex.toMap
//  vocab.foreach(println)

//  // Convert documents into term count vectors
//  val documents: RDD[(Long, Vector)] =
//    tokenized.zipWithIndex.map {
//      case (tokens, id) =>
//      val counts = new mutable.HashMap[Int, Double]()
//      tokens.foreach { term =>
//        if (vocab.contains(term)) {
//          val idx = vocab(term)
//          counts(idx) = counts.getOrElse(idx, 0.0) + 1.0
//        }
//      }
//      (id, Vectors.sparse(vocab.size, counts.toSeq))
//    }
//
//  documents.foreach(println)
//
//  // Set LDA parameters
//  val numTopics = 3
//  val lda = new LDA().setK(numTopics).setMaxIterations(8)
//
//  val ldaModel = lda.run(documents)
////  val avgLogLikelihood = ldaModel.logLikelihood / documents.count()
//
//  // Print topics, showing top-weighted 10 terms for each topic.
//  val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
//  topicIndices.foreach { case (terms, termWeights) =>
//    println("TOPIC:")
//    terms.zip(termWeights).foreach { case (term, weight) =>
//      println(s"${vocabArray(term.toInt)}\t$weight")
//    }
//    println()
//  }
  //
}
