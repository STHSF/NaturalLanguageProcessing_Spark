

/**
  * Created by li on 16/7/14.
  */
object r2016 {


  def main(args: Array[String]) {

//    val conf = new SparkConf().setAppName("map flatMap").setMaster("local")
//    val sc = new SparkContext(conf)

    val str = Array("iii ewewew ddd ddfddf dddd ddddas weewwe wewer fdggfg ewterewt", "asfas fgrg jgiuiog hgukguy hjguy bhkui")

    val res1 = str.map(x => x.split(" "))

    val res2 = str.flatMap(x => x.split(" "))


  }

}
