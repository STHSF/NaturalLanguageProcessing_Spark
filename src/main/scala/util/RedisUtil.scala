package util

import redis.clients.jedis.Jedis

import scala.collection.mutable
import scala.xml.XML

/**
  * Created by li on 16/7/8.
  */
object RedisUtil {

  var jedis: Jedis = null
  /**
    * 初始化 redis
    *
    * @param confDir 配置文件对应的 xml 对象
    * @note rowNum: 10
    */
  def initRedis(confDir: String):Unit = {

    val configFile = XML.loadFile(confDir)

    val redisIp = (configFile \ "redis" \ "ip").text
    val redisPort = (configFile \ "redis" \ "port").text.toInt
    val redisDB = (configFile \ "redis" \ "db").text.toInt
    val redisAuth = (configFile \ "redis" \ "auth").text

    jedis = new Jedis(redisIp, redisPort)
    jedis.auth(redisAuth)
    jedis.select(redisDB)
  }

  /**
    * 将结果保存到redis
    *
    * @param  resultData 需要保存的数据
    * @author LiYu
    * @note rowNum: 12
    */
  def write2Redis(resultData: Array[(String, String)], time: String, dataType: String): Unit = {

    val  resultDataMap = mutable.HashMap[String, String]()

    resultData.foreach{line => {
      resultDataMap.put(line._1, line._2)
    }}

    val pipeline = jedis.pipelined()

    resultDataMap.toSeq.foreach{ x => {

      pipeline.hset(s"vipstockstatistic_$dataType" + s"_$time", x._1, x._2)
      //      pipeline.expire("hotwordsrank_test:", 60 * 60 * 12)
    }}

    pipeline.sync()
  }


}
