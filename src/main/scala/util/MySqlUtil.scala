package util

import java.sql.{ResultSet, PreparedStatement, Connection, DriverManager}

/**
  * Created by li on 16/7/12.
  */
object MySQLUtil {

  def getConnect: Connection ={

    //写在配置文件中
    val url = ""
    val userName = ""
    val password = ""

    //设置驱动
    Class.forName("com.mysql.jdbc.Driver")
    val conn = DriverManager.getConnection(url, userName, password)

    conn
  }


  def write2Mysql(data: Iterator[String], sql: String): Unit ={

    var conn: Connection = null
    var ps: PreparedStatement = null

    try{

      conn = getConnect()

      data.foreach{
        line => {

          val temp = line.split(",") // 需要修改

          ps = conn.prepareStatement(sql)
          ps.setString(1, temp(0))
          ps.setString(2, temp(1))

          ps.executeUpdate()
        }
      }
    } catch{

      case e: Exception => println("Mysql Exception")
    } finally {

      if(conn != null) {

        conn.close()
      }

      if(ps != null) {

        ps.close()
      }
    }
  }

  def readFromMysql(sql: String): Unit = {

    var conn: Connection = null
    try {

      conn = getConnect()

      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
      val result = statement.executeQuery(sql)

      while(result.next()) {

        /**
          * todo
          */
        result.getString("quote") //table name

      }

    } catch {

      case e: Exception => println("read fault")
    } finally {

      conn.close()
    }
  }



}
