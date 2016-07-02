package com.kunyan.scheduler

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, ArrayBuffer}

/**
  * Created by li on 16/6/29.
  */
object HotWordsTest extends App {

  val conf = new SparkConf().setAppName("test").setMaster("local")
  val sc = new SparkContext(conf)

  val a = ("url1", List("程序员","专业"))
  val b = ("url2", List("专业","代码"))
  val c = ("url3", List("方面","代码"))


  val d = ("url1", "程序,专业,方面,程序员")
  val e = ("url2", "专业,代码,程序")
  val f = ("url3", "专业,代码,方面,专业")



  val g = ("url1", "银行 代码 金融")
  val h = ("url2", "电力 爬虫 计算机")
  val k = ("url3", "电力 代码 计算机")



  //  def getWordRank(eventWordsList: RDD[(String, List[String])],
  //                  newStateFilter: RDD[(String, String)]): RDD[(String, Seq[(String, Int)])] = {
  //
  //    val temp = newStateFilter.map(_._2).map(x => x.split(",")).collect
  //    val eventWord = mutable.HashMap[String, Int]()
  //
  //    val res = eventWordsList.map {
  //      event =>{
  //        val key = event._1
  //        val eventWords = event._2
  //        eventWords.foreach {
  //          word => {
  //            for(item <- temp.indices) {
  //              if (temp(item).contains(word)) {
  //                val value = eventWord.getOrElseUpdate(word, 0)
  //                eventWord.update(word, value + 1)
  //              }
  //            }
  //          }
  //        }
  //
  //        val a = (key, eventWord.toSeq)
  //        print(a._1)
  //        a._2.foreach(x => println((x._1, x._2)))
  //
  //        (key, eventWord.toSeq)
  //      }
  //    }
  //
  //    res
  //  }


  def getWordRank(sc: SparkContext,
                  eventWordsList: RDD[(String, List[String])],
                  newStateFilter: RDD[(String, String)]): Seq[(String, (String, Int))] = {

    val res = new ArrayBuffer[(String, (String, Int))]

    val aa = eventWordsList.map(_._2).flatMap(x => x).distinct().collect()

    val cc = newStateFilter.map(line => (line._1, line._2.split(",").distinct))

    val bb = cc.map {
      word =>
        val url = word._1
        val filteredWords = word._2.filter(x => aa.contains(x)).map((_, 1))
        (url, filteredWords)
    }.cache()

    val wordf = bb.flatMap(_._2).reduceByKey(_ + _).collect()
    val broadcastWordf = sc.broadcast(wordf)

    val eventWordsRepositoryMap = mutable.HashMap[String, Int]()
    broadcastWordf.value.foreach{
      line => {
        val words = line._1
        val df = line._2
        eventWordsRepositoryMap.put(words, df)
      }
    }

    eventWordsList.collect().foreach {
      line => {
        val url = line._1
        line._2.foreach {
          word => {
            val temp = eventWordsRepositoryMap.get(word).get
            res.+=((url, (word, temp)))
          }
        }
      }
    }

    res.toSeq
  }




  def entityWordsGet(propertyTable: RDD[(String, String)]): (RDD[(Seq[String], (String, Int))],
    RDD[(Seq[String], (String, Int))],
    RDD[(Seq[String], (String, Int))]) = {

    val entity = propertyTable.map {
      line => {
        val url = line._1
        val stockTemp = line._2.split(" ")(0)
        val industryTemp = line._2.split(" ")(1)
        val sectionTemp = line._2.split(" ")(2)
        ((stockTemp, industryTemp, sectionTemp ), url)

      }
    }

    val stockUrl = entity.map(x => (x._1._1, x._2)).groupByKey()
    val industryUrl = entity.map(x => (x._1._2, x._2)).groupByKey()
    val sectionUrl = entity.map(x => (x._1._3, x._2)).groupByKey()

    val stockUrlCount = stockUrl.map(line =>{
      val urlList = line._2.toSeq
      val stockProperty = line._1
      val df = line._2.size
      (urlList, (stockProperty , df))
    })

    val sectionUrlCount = sectionUrl.map(line =>{
      (line._2.toSeq, (line._1, line._2.size))
    })

    val industryUrlCount = industryUrl.map(line =>{
      (line._2.toSeq, (line._1, line._2.size))
    })

    val stockUrlList = stockUrlCount.flatMap(
      line =>{
        val list = new ListBuffer[(String, (String, Int))]

        line._1.foreach(x => {

          list.+=((x,line._2))
        })

        list
      }
    ).collect()

    val industryUrlList = industryUrlCount.flatMap(
      line =>{
        val list = new ListBuffer[(String, (String, Int))]

        line._1.foreach(x => {

          list.+=((x,line._2))
        })

        list
      }
    ).collect()

    val sectionUrlList = sectionUrlCount.flatMap(
      line =>{
        val list = new ListBuffer[(String, (String, Int))]

        line._1.foreach(x => {

          list.+=((x,line._2))
        })

        list
      }
    ).collect()

    val propertyLibList = stockUrlList.++:(industryUrlList).++:(sectionUrlList).toMap

    (stockUrlCount, industryUrlCount, sectionUrlCount)
  }




  def entityWordsUnionEventWords(entityWords: (RDD[(String, Iterable[String], Int)],
    RDD[(String, Iterable[String], Int)],
    RDD[(String, Iterable[String], Int)]),
                                 eventWords: Seq[(String, (String, Int))]): (Seq[(String, String, Int)],
    Seq[(String, String, Int)],
    Seq[(String, String, Int)]) = {

    // 通过url合并实体词和事件词,整理成[属性,Seq(词项, 词频)]的格式
    val stockWordsUrl = entityWords._1
    val industryWordsUrl = entityWords._2
    val sectionWordsUrl = entityWords._3

    val stockWords = new ArrayBuffer[(String, String, Int)]
    stockWordsUrl.collect().foreach {
      line => {
        eventWords.foreach (
          x => {
            if (line._2.toArray.contains(x._1)) {
              stockWords.+=((line._1, x._2._1, x._2._2))
            }
          }
        )
      }
    }

    val industryWords = new ArrayBuffer[(String, String, Int)]
    industryWordsUrl.collect().foreach {
      line => {
        eventWords.foreach (
          x => {
            if (line._2.toArray.contains(x._1)) {
              industryWords.+=((line._1, x._2._1, x._2._2))
            }
          }
        )
      }
    }

    val sectionWords = new ArrayBuffer[(String, String, Int)]
    sectionWordsUrl.collect().foreach {
      line => {
        eventWords.foreach (
          x => {
            if (line._2.toArray.contains(x._1)) {
              sectionWords.+=((line._1, x._2._1, x._2._2))
            }
          }
        )
      }
    }

    (stockWords.toSeq, industryWords.toSeq, sectionWords.toSeq)
  }


  val a1 = List(("程序", 3), ("程序员", 4), ("测试", 2))
  val a2 = List(("测试", 2), ("程序", 4), ("银行", 10))


  /**
    *
    * @param hotWords
    * @param preHotWords
    */
  def bayesianAverage(hotWords: RDD[(String, Int)],
                      preHotWords: RDD[(String, Int)]): Seq[(String, Float)] ={

    //Atp(w): 当前词频
    val hotWord = hotWords.map(_._1)

    //Btp(w): 历史词频
    val preHotWord = preHotWords.map(_._1)

    val wordLib = hotWords.++(preHotWords)

    //TpSum: 词频和
    val wordLibArray = wordLib.reduceByKey(_ + _).collect()

    //TpAvg:词频和的平均
    val tpSum = wordLibArray.map(_._2).sum
    val tpAvg = tpSum.toFloat / wordLibArray.length

    //Atp(w)/TpSum 当前词频与词频和比值
    val resultMap = new mutable.HashMap[String, Float]
    val atp = hotWords.collect().toMap
    val wordLibMap = wordLibArray.toMap
    wordLibMap.foreach {
      line =>{
        if (atp.contains(line._1)){
          val temp = atp.get(line._1).get
          val item = temp.toFloat / line._2
          resultMap.put(line._1, item)
        } else {
          resultMap.put(line._1, 0f)
        }
      }
    }

    //R(avg) 当前词频与词频和比值的平均值
    val rAvg = resultMap.values.toArray.sum / resultMap.values.size

    // 热度计算
    val result = new mutable.HashMap[String, Float]
    wordLibMap.foreach {
      line => {
        val res1 = wordLibMap.get(line._1).get
        val res2 = resultMap.get(line._1).get
        val value = (res1 * res2 + tpAvg * rAvg) / (res1 + tpAvg)
        result.put(line._1, value)
      }
    }

    result.toSeq
  }




  def fileProcessing(sc: SparkContext, dir: String): Array[(String, (String, Int))] = {



  }





  val data = sc.parallelize(List(a, b, c))
  val data2 = sc.parallelize(List(d, e, f))
  val data3 = sc.parallelize(List(g, h, k))

  val data4 = sc.parallelize(a1)
  val data5 = sc.parallelize(a2)

//  // 事件词词频
//  val reslut1 = getWordRank(sc, data, data2)
//
//  // 实体词词频
  val result2 =  entityWordsGet(data3)
//
//  // 实体词事件词揉合
//  val result3 = entityWordsUnionEventWords(result2, reslut1)
//  result3._1.foreach(x => println("stock" + x._1, x._2, x._3))
//  result3._2.foreach(x => println("industry" + x._1, x._2, x._3))
//  result3._3.foreach(x => println("section" + x._1, x._2, x._3))

  val result5 = bayesianAverage(data4, data5)


}
