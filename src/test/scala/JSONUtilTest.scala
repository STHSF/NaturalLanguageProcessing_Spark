import util.JSONUtil

/**
  * Created by li on 16/8/29.
  */
object JSONUtilTest {


  def main(args: Array[String]) {

    val confDir = "/Users/li/Kunyan/NaturalLanguageProcessing/src/main/resources/jsonConfig.json"

    JSONUtil.initConfig(confDir)

    val res = JSONUtil.getValue("hbase", "rootDir")

    println(res)
  }

}
