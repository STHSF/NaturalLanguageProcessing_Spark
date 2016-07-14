package util

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import scala.xml.{XML, Elem}

/**
  * Created by li on 16/7/12.
  */
object MySQLUtil {


  /**
    * 获取xml格式的配置文件
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
  def getConnect(configFile: Elem): Connection ={

    //写在配置文件中
    val url = (configFile \ "mysql" \ "url" ).text
    val userName = (configFile \ "mysql" \ "username").text
    val password = (configFile \ "mysql" \ "password").text

    //设置驱动
    Class.forName("com.mysql.jdbc.Driver")

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

      // data 需要写入的内容
      data.foreach{ line => {

        // 对data的每一行进行操作
        val temp = line.split(",")

        prep = conn.prepareStatement(sql)
        prep.setString(1, temp(0))
        prep.setString(2, temp(1))

        prep.executeUpdate()
      }}
    } catch{

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
  def readFromMysql(configFile: Elem, sql: String): Unit = {

    var conn: Connection = null

    try {

      // 读取配置文件并建立连接
      conn = getConnect(configFile)

      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
      // 通过sql查询语句查询的结果
      val result = statement.executeQuery(sql)

      while(result.next()) {

        /**
          * todo 对查询的结果进行操作
          */
        result.getString("quote") //table name

      }

    } catch {

      case e: Exception => println("Mysql Read Fault")
    } finally {

      conn.close()
    }
  }



}
