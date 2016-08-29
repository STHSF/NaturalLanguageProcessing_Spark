package util

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import scala.collection.mutable.ArrayBuffer

import scala.xml.{XML, Elem}

/**
  * Created by li on 16/7/12.
  */
object MySQLUtil {

  /**
    * 获取xml格式的配置文件
    *
    * @param dir 配置文件所在的文件目录
    * @return
    */
  def readConfigFile(dir: String): Elem = {

    val configFile = XML.loadFile(dir)

    configFile
  }

  /**
    * 读取配置文件中的内容,并建立连接
    *
    * @param configFile 配置文件
    * @return
    */
  def getConnect(configFile: Elem): Connection = {

    //写在配置文件中
    val url = (configFile \ "mysql" \ "url" ).text
    val userName = (configFile \ "mysql" \ "username").text
    val password = (configFile \ "mysql" \ "password").text

    //设置驱动
    Class.forName("com.mysql.jdbc.Driver")

    //初始化
    val conn = DriverManager.getConnection(url, userName, password)

    conn
  }

  /**
    * 向mysql中写数据
    *
    * @param configFile 配置文件
    * @param data 需要写进数据库里面的数据
    * @param sql sql查询语句, 格式(sql = "INSERT INTO quotes (quote, author) VALUES (?, ?)")
    */
  def write2Mysql(configFile: Elem, data: Iterator[String], sql: String): Unit ={

    var conn: Connection = null
    var prep: PreparedStatement = null

    try{

      // 读取配置文件并建立连接
      conn = getConnect(configFile)

      /** 对需要写入的内容(data)的每一行进行操作 */
      data.foreach{ line => {

        val temp = line.split(",")

        /** sql插入语句: */
        prep = conn.prepareStatement(sql)
        prep.setString(1, temp(0))
        prep.setString(2, temp(1))

        prep.executeUpdate()
      }}
    } catch {

      case e: Exception => println("Mysql Exception")
    } finally {

      if(conn != null) {

        conn.close()
      }

      if(prep != null) {

        prep.close()
      }
    }
  }

  /**
    * 从mysql中读取数据
    *
    * @param configFile 配置文件
    * @param sql mysql查询语句
    */
  def readFromMysql(configFile: Elem, sql: String): Array[(String, String)] = {

    var conn: Connection = null

    try {

      // 读取配置文件并建立连接
      conn = getConnect(configFile)

      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
      // 通过sql查询语句查询的结果
      // val sql = "select symbol, sename from bt_stcode where (EXCHANGE = '001002' or EXCHANGE = '001003') and SETYPE = '101' and CUR = 'CNY' and ISVALID = 1 and LISTSTATUS <> '2'"
      val result = statement.executeQuery(sql)

      val stocks = ArrayBuffer[(String, String)]()
      while(result.next()) {

        /** todo 对查询的结果进行操作 */
        val stockID = result.getString("symbol") // symbol: row name
        val stock = stockID + "," + result.getString("sename") // sename: row name
        stocks +=((stockID, stock))
      }

      stocks.toArray
    } catch {

      case e: Exception => Array(("error", "error"))
    } finally {

      conn.close()
    }
  }

}
