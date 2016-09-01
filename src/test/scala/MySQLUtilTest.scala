import org.apache.spark.{SparkContext, SparkConf}
import util.{XMLUtil, MySQLUtil}

/**
  * Created by li on 16/8/29.
  */
object MySQLUtilTest {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MySQLUtilTest").setMaster("local")
    val sc = new SparkContext(conf)

    val confDir = "/Users/li/Kunyan/workShop/VipStockStatistic/src/main/scala/util/config.xml"

    val stockSql = "select symbol, sename from bt_stcode where (EXCHANGE = '001002' or EXCHANGE = '001003') " +
      "and SETYPE = '101' and CUR = 'CNY' and ISVALID = 1 and LISTSTATUS <> '2'"

    val configFile = XMLUtil.readConfigFile(confDir)

    val stockDic = MySQLUtil.readFromMysql(configFile, stockSql)
      .map(row => (row._1, row._2.split(","))).toMap

    stockDic.foreach(x => print(x._1, x._2(0)))






  }

}
