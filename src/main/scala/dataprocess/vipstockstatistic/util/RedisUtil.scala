package dataprocess.vipstockstatistic.util

import redis.clients.jedis.Jedis

import scala.xml.XML

/**
  * Created by li on 16/8/23.
  */
object RedisUtil {

  var jedis: Jedis = null

  /**
    * 初始化 redis
    *
    * @param confDir 配置文件对应的 xml 对象
    * @note rowNum: 10
    */
  def initRedis(confDir: String): Jedis = {

    val configFile = XML.loadFile(confDir)

    val redisIp = (configFile \ "redis" \ "ip").text
    val redisPort = (configFile \ "redis" \ "port").text.toInt
    val redisDB = (configFile \ "redis" \ "db").text.toInt
    val redisAuth = (configFile \ "redis" \ "auth").text

    jedis = new Jedis(redisIp, redisPort)
    jedis.auth(redisAuth)
    jedis.select(redisDB)

    jedis
  }

  /**
    * 将结果保存到redis
    *
    * @param  resultData 需要保存的数据
    * @author Li Yu
    * @note rowNum: 12
    */
  def write2Redis(resultData: Array[(String, String)], time: String, dataType: String, confDir: String): Unit = {

    val jedis = initRedis(confDir)

    resultData.foreach{ x => {

      jedis.zadd(s"vipstockstatistic_$dataType" + s"_$time", x._2.toDouble, x._1)
    }}
  }

}
