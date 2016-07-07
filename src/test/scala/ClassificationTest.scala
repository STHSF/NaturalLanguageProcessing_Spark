import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by li on 16/3/30.
  */
object ClassificationTest {

  val conf = new SparkConf().setAppName("test")
  val sc = new SparkContext(conf)

  val url_context = new ListBuffer[( String, String)]
  val  url_catagory  = new mutable.HashMap[String, String]
  val aa = new mutable.HashMap[String, String]

  /**
    * 将输入数据转化成ListBuffer格式
    *
    * @param line 需要处理的字符串
    * @return ListBuffer格式的数据
    */
  def flatmap2Array(line:String):ListBuffer[(String,String)]={
    val arr  = line.split("\t")
    if(arr.length > 1)
      url_context.+=((arr(0),arr(1)))
    url_context

  }

  /**
    * 将输入数据转化成HashMap格式
    *
    * @param line 需要处理的字符串
    * @return HashMap格式的数据
    */
  def flatmap2Map(line:String):mutable.HashMap[String,String]={
    val arr  = line.split("\t")
    if(arr.length>1)
      aa.+=(arr(0) -> arr(1))
    aa
  }


  /**
    * 将url_content和url_catagory合并,并且按照制定的catagory标签
    *
    * @param list 输入数据
    * @param map 输入数据
    * @param catagory 行业名称
    * @param dataFile 输出的文件路径
    * @return HashMap格式的数据
    */
  def getTrainingset(list:ListBuffer[(String,String)],map:mutable.HashMap[String,String],catagory: String, dataFile:String): Unit ={

    //    val trainingSet = new ListBuffer[String ]
    //    for(item <- list) {
    //
    //      // var url = item._1
    //      // var cata = map.get(url).get
    //      // var indu =item._2
    //      // url = if(cata == catagory) "1" else "0"
    //
    //      val cata = map.get(item._1).get
    //      val trainingdata = (if(cata == catagory) "1" else "0") + "\t" + cata + "\t"+ item._2
    //      //      val trainingSet = (item._1, map.get(item._1).get, item._2)
    //      //      val training = trainingSet._1 + "\t" + trainingSet._2 + "\t" + trainingSet._3
    //      //      println(trainingSet)
    //      //      println(trainingdata)
    //      trainingSet += trainingdata
    //    }
    //    trainingSet
    //    println(data)

    val DataFile = new File(dataFile)
    val bufferWriter = new BufferedWriter(new FileWriter(DataFile))
    for(item <- list) {
      val cata = map.get(item._1).get
      bufferWriter.write((if(cata == catagory) "1" else "0") + "\t" + cata + "\t"+ item._2 + "\n")
    }
    bufferWriter.flush()
    bufferWriter.close()
  }

  def main(args: Array[String]) {
    //    val urlContent  = sc.textFile("/users/li/Downloads/segTraining").flatMap(flatmap2Array).collect()
    //    val urlcatagory  = sc.textFile("/users/li/Downloads/traininglabel").flatMap(flatmap2Map).collect()

    val urlContent  = sc.textFile("file:///home/liyu/nlp/testset/segTraining").flatMap(flatmap2Array).collect()
    val urlcatagory  = sc.textFile("file:///home/liyu/nlp/testset/traininglabel")
      .flatMap(flatmap2Map)
      .collect()
      .foreach(x => url_catagory += (x._1 -> x._2))

    //    val urlContent  = sc.textFile("file:///home/liyu/nlp/trainingdataset/segTrainingSet").flatMap(flatmap2Array).collect()
    //        val urlcatagory  = sc.textFile("file:///home/liyu/nlp/trainingdataset/traininglabel")
    //          .flatMap(flatmap2Map)
    //          .collect()
    //          .foreach(x => url_catagory += (x._1 -> x._2))

    println("计算中-------------")
    val catagory1 = "有色金属"
    val datafile1 = "/home/liyu/nlp/YSJS.txt"
//    val datafile1 = "/users/li/Downloads/YSJS.txt"
    getTrainingset(url_context, url_catagory,catagory1 , datafile1)
    println("第1个文件保存完毕--------")

    //    val catagory2 = "化工化纤"
    //    val datafile2 = "file:///home/liyu/nlp/HGHQ.txt"
    //    getTrainingset(url_context, url_catagory, catagory2, datafile2)
    //    println("第2个文件保存完毕--------")
    //
    //    val catagory3 = "供水供气"
    //    val datafile3 = "file:///home/liyu/nlp/GSGQ.txt"
    //    getTrainingset(url_context, url_catagory, catagory3, datafile3)
    //    println("第3个文件保存完毕--------")
    //
    //    val catagory4 = "仪电仪表"
    //    val datafile4 = "file:///home/liyu/nlp/YSYB.txt"
    //    getTrainingset(url_context, url_catagory, catagory4, datafile4)
    //    println("第4个文件保存完毕--------")
    //
    //    val catagory5 = "通信"
    //    val datafile5 = "file:///home/liyu/nlp/TX.txt"
    //    getTrainingset(url_context, url_catagory, catagory5, datafile5)
    //    println("第5个文件保存完毕--------")
    //
    //    val catagory6 = "商业连锁"
    //    val datafile6 = "file:///home/liyu/nlp/SYLS.txt"
    //    getTrainingset(url_context, url_catagory, catagory6, datafile6)
    //    println("第6个文件保存完毕--------")
    //
    //    val catagory7 = "机械"
    //    val datafile7 = "file:///home/liyu/nlp/JX.txt"
    //    getTrainingset(url_context, url_catagory, catagory7, datafile7)
    //    println("第7个文件保存完毕--------")
    //
    //    val catagory8 = "运输物流"
    //    val datafile8 = "file:///home/liyu/nlp/YSWL.txt"
    //    getTrainingset(url_context, url_catagory, catagory8, datafile8)
    //    println("第8个文件保存完毕--------")
    //
    //    val catagory9 = "纺织服装"
    //    val datafile9 = "file:///home/liyu/nlp/FZFZ.txt"
    //    getTrainingset(url_context, url_catagory, catagory9, datafile9)
    //    println("第9个文件保存完毕--------")
    //
    //    val catagory10 = "钢铁"
    //    val datafile10 = "file:///home/liyu/nlp/GT.txt"
    //    getTrainingset(url_context, url_catagory, catagory10, datafile10)
    //    println("第10个文件保存完毕--------")
    //
    //    val catagory11 = "交通工具"
    //    val datafile11 = "file:///home/liyu/nlp/JTGJ.txt"
    //    getTrainingset(url_context, url_catagory, catagory11, datafile11)
    //    println("第11个文件保存完毕--------")
    //
    //    val catagory12 = "医药"
    //    val datafile12 = "file:///home/liyu/nlp/YY.txt"
    //    getTrainingset(url_context, url_catagory, catagory12, datafile12)
    //    println("第12个文件保存完毕--------")
    //
    //    val catagory13 = "煤炭石油"
    //    val datafile13 = "file:///home/liyu/nlp/MTSY.txt"
    //    getTrainingset(url_context, url_catagory, catagory13, datafile13)
    //    println("第13个文件保存完毕--------")
    //
    //    val catagory14 = "酿酒食品"
    //    val datafile14 = "file:///home/liyu/nlp/NJSP.txt"
    //    getTrainingset(url_context, url_catagory, catagory14, datafile14)
    //    println("第14个文件保存完毕--------")
    //
    //    val catagory15 = "电器"
    //    val datafile15 = "file:///home/liyu/nlp/DQ.txt"
    //    getTrainingset(url_context, url_catagory, catagory15, datafile15)
    //    println("第15个文件保存完毕--------")
    //
    //    val catagory16 = "建材"
    //    val datafile16 = "file:///home/liyu/nlp/JC.txt"
    //    getTrainingset(url_context, url_catagory, catagory16, datafile16)
    //    println("第16个文件保存完毕--------")
    //
    //    val catagory17 = "旅游酒店"
    //    val datafile17 = "file:///home/liyu/nlp/LYFW.txt"
    //    getTrainingset(url_context, url_catagory, catagory17, datafile17)
    //    println("第17个文件保存完毕--------")
    //
    //    val catagory18 = "电力"
    //    val datafile18 = "file:///home/liyu/nlp/DL.txt"
    //    getTrainingset(url_context, url_catagory, catagory18, datafile18)
    //    println("第18个文件保存完毕--------")
    //
    //    val catagory19 = "电子信息"
    //    val datafile19 = "file:///home/liyu/nlp/DZXX.txt"
    //    getTrainingset(url_context, url_catagory, catagory19, datafile19)
    //    println("第19个文件保存完毕--------")
    //
    //    val catagory20 = "交通设施"
    //    val datafile20 = "file:///home/liyu/nlp/JTSS.txt"
    //    getTrainingset(url_context, url_catagory,catagory20 , datafile20)
    //    println("第20个文件保存完毕--------")
    //
    //    val catagory21 = "工程建筑"
    //    val datafile21 = "file:///home/liyu/nlp/GCJZ.txt"
    //    getTrainingset(url_context, url_catagory,catagory21 , datafile21)
    //    println("第21个文件保存完毕--------")
    //
    //    val catagory22 = "农林牧渔"
    //    val datafile22 = "file:///home/liyu/nlp/NLMY.txt"
    //    getTrainingset(url_context, url_catagory,catagory22 , datafile22)
    //    println("第22个文件保存完毕--------")
    //
    //    val catagory23 = "造纸印刷"
    //    val datafile23 = "file:///home/liyu/nlp/ZZYS.txt"
    //    getTrainingset(url_context, url_catagory,catagory23 , datafile23)
    //    println("第23个文件保存完毕--------")
    //
    //    val catagory24 = "计算机"
    //    val datafile24 = "file:///home/liyu/nlp/JSJ.txt"
    //    getTrainingset(url_context, url_catagory,catagory24 , datafile24)
    //    println("第24个文件保存完毕--------")
    //
    //    val catagory25 = "有色金属"
    //    val datafile25 = "file:///home/liyu/nlp/YSJS.txt"
    //    getTrainingset(url_context, url_catagory,catagory25 , datafile25)
    //    println("第25个文件保存完毕--------")
    //
    //    val catagory26 = "保险"
    //    val datafile26 = "file:///home/liyu/nlp/BX.txt"
    //    getTrainingset(url_context, url_catagory,catagory26 , datafile26)
    //    println("第26个文件保存完毕--------")
    //
    //    val catagory27 = "券商"
    //    val datafile27 = "file:///home/liyu/nlp/QS.txt"
    //    getTrainingset(url_context, url_catagory,catagory27 , datafile27)
    //    println("第27个文件保存完毕--------")
    //
    //    val catagory28 = "其他行业"
    //    val datafile28 = "file:///home/liyu/nlp/QTHY.txt"
    //    getTrainingset(url_context, url_catagory,catagory28 , datafile28)
    //    println("第28个文件保存完毕--------")
    //
    //    val catagory29 = "银行类"
    //    val datafile29 = "file:///home/liyu/nlp/YHL.txt"
    //    getTrainingset(url_context, url_catagory,catagory29 , datafile29)
    //    println("第29个文件保存完毕--------")
    //
    //    val catagory30 = "非银金融"
    //    val datafile30 = "file:///home/liyu/nlp/FYJR.txt"
    //    getTrainingset(url_context, url_catagory,catagory30 , datafile30)
    //    println("第30个文件保存完毕--------")
    //
    //    val catagory31 = "教育传媒"
    //    val datafile31 = "file:///home/liyu/nlp/JYCM.txt"
    //    getTrainingset(url_context, url_catagory,catagory31 , datafile31)
    //    println("第31个文件保存完毕--------")
    //
    //    val catagory32 = "外贸"
    //    val datafile32 = "file:///home/liyu/nlp/WM.txt"
    //    getTrainingset(url_context, url_catagory,catagory32 , datafile32)
    //    println("第32个文件保存完毕--------")
    //
    //


  }

  //  sc.stop()
}