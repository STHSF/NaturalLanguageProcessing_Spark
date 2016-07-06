package util

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * Created by li on 16/3/31.
  * 训练数据转化能够实现从本地读取两个固定格式的文件
  * 挑选训练集中的数据,按照catagory挑选,当选定一个catagory时,将url替换成并标签设置为1,其余的catagory的url标签设置为0
  * 保存成[1/0, catagory, content],按不同的catagory分别保存.
  */
object DataTransform {

  val conf = new SparkConf().setAppName("classificationSF").setMaster("local")
  val sc = new SparkContext(conf)
  val trainingSet = new ListBuffer[(String, String, String)]


  /**
    *
    * @param url 使用读取文件
    * @return
    */


  def getFile(url: String): Array[(String, String)]={
    val content = Source.fromFile(url).getLines().toArray.map{
      line =>
        val data  =  line.split("\t")
        if (data.length > 1) data(0) -> data(1)
    }.filter( _ != ()).map(_.asInstanceOf[(String, String)])
    content
  }


  def getfile(url: String): RDD[(String, String)] = {
    val content = sc.textFile(url).map{
      line =>
        val data  =  line.split("\t")
        if (data.length > 1) data(0) -> data(1)
    }.filter( _ != ()).map(_.asInstanceOf[(String, String)])
    content
  }

  def getTrainingSet(catagory: Array[(String, String)], content: Array[(String, String)], label: String, dataFile: String): Unit ={

    val data = new ListBuffer[(String, String, String)]
    //    val trainingSet = new ListBuffer[(String, String, String)]

    // catagory[url, catagory]
    // content[url, content]
    // 将catagory 和content 内的数据通过url合并在一起
    content.foreach(
      line1 =>
        catagory.foreach(
          line2 =>
            if(line2._1 == line1._1 ){
              data +=((line1._1, line2._2, line1._2))
            }
        )
    )

    //    // 将url变换成0/1标签并保存到ListBuffer中
    //    data.foreach(
    //      line =>
    //        trainingSet.+=((if(line._2 == label) "1" else "0", line._2, line._3))
    //    )
    //    trainingSet.foreach(println)

    val DataFile = new File(dataFile)
    val bufferWriter = new BufferedWriter(new FileWriter(DataFile))
    // 将url变换成0/1标签并保存到文件里面,保存格式[1/0  catagory  content]
    data.foreach{
      line =>
        bufferWriter.write((if (line._2 == label) "1" else "0") + "\t"+ line._2 + "\t" + line._3 + "\n" )
    }
    bufferWriter.flush()
    bufferWriter.close()

  }


  def main(args: Array [String]){

    val url1 = "/users/li/Downloads/segTrainingSet"
    val url2 = "/users/li/Downloads/trainglabel3.txt"

    //    val url1 = "/home/liyu/nlp/testset/segTraining"
    //    val url2 = "/home/liyu/nlp/testset/traininglabel"
    //    val url1 = "/home/liyu/nlp/trainingdataset/segTrainingSet"
    //    val url2 = "/home/liyu/nlp/trainingdataset/traininglabel"
    val urlContent = getFile(url1)
    val urlCatagory = getFile(url2)

//    urlCatagory.foreach(println)

    println("计算中-------------")
    val catagory1 = "有色金属"
    //    val datafile1 = "/home/liyu/nlp/YSJS.txt"
    val datafile1 = "/users/li/Downloads/traningset/YSJS.txt"
    getTrainingSet(urlCatagory, urlContent, catagory1 , datafile1)
    println("第1个文件保存完毕------")

    val catagory2 = "化工化纤"
    val datafile2 = "/Users/li/Downloads/traningset/HGHQ.txt"
    getTrainingSet(urlCatagory, urlContent, catagory2, datafile2)
    println("第2个文件保存完毕--------")

    val catagory3 = "供水供气"
    val datafile3 = "/Users/li/Downloads/traningset/GSGQ.txt"
    getTrainingSet(urlCatagory, urlContent, catagory3, datafile3)
    println("第3个文件保存完毕--------")

    val catagory4 = "仪电仪表"
    val datafile4 = "/Users/li/Downloads/traningset/YSYB.txt"
    getTrainingSet(urlCatagory, urlContent, catagory4, datafile4)
    println("第4个文件保存完毕--------")

    val catagory5 = "通信"
    val datafile5 = "/Users/li/Downloads/traningset/TX.txt"
    getTrainingSet(urlCatagory, urlContent, catagory5, datafile5)
    println("第5个文件保存完毕--------")

    val catagory6 = "商业连锁"
    val datafile6 = "/Users/li/Downloads/traningset/SYLS.txt"
    getTrainingSet(urlCatagory, urlContent, catagory6, datafile6)
    println("第6个文件保存完毕--------")

    val catagory7 = "机械"
    val datafile7 = "/Users/li/Downloads/traningset/JX.txt"
    getTrainingSet(urlCatagory, urlContent, catagory7, datafile7)
    println("第7个文件保存完毕--------")

    val catagory8 = "运输物流"
    val datafile8 = "/Users/li/Downloads/traningset/YSWL.txt"
    getTrainingSet(urlCatagory, urlContent, catagory8, datafile8)
    println("第8个文件保存完毕--------")

    val catagory9 = "纺织服装"
    val datafile9 = "/Users/li/Downloads/traningset/FZFZ.txt"
    getTrainingSet(urlCatagory, urlContent, catagory9, datafile9)
    println("第9个文件保存完毕--------")

    val catagory10 = "钢铁"
    val datafile10 = "/Users/li/Downloads/traningset/GT.txt"
    getTrainingSet(urlCatagory, urlContent, catagory10, datafile10)
    println("第10个文件保存完毕--------")

    val catagory11 = "交通工具"
    val datafile11 = "/Users/li/Downloads/traningset/JTGJ.txt"
    getTrainingSet(urlCatagory, urlContent, catagory11, datafile11)
    println("第11个文件保存完毕--------")

    val catagory12 = "医药"
    val datafile12 = "/Users/li/Downloads/traningset/YY.txt"
    getTrainingSet(urlCatagory, urlContent, catagory12, datafile12)
    println("第12个文件保存完毕--------")

    val catagory13 = "煤炭石油"
    val datafile13 = "/Users/li/Downloads/traningset/MTSY.txt"
    getTrainingSet(urlCatagory, urlContent, catagory13, datafile13)
    println("第13个文件保存完毕--------")

    val catagory14 = "酿酒食品"
    val datafile14 = "/Users/li/Downloads/traningset/NJSP.txt"
    getTrainingSet(urlCatagory, urlContent, catagory14, datafile14)
    println("第14个文件保存完毕--------")

    val catagory15 = "电器"
    val datafile15 = "/Users/li/Downloads/traningset/DQ.txt"
    getTrainingSet(urlCatagory, urlContent, catagory15, datafile15)
    println("第15个文件保存完毕--------")

    val catagory16 = "建材"
    val datafile16 = "/Users/li/Downloads/traningset/JC.txt"
    getTrainingSet(urlCatagory, urlContent, catagory16, datafile16)
    println("第16个文件保存完毕--------")

    val catagory17 = "旅游酒店"
    val datafile17 = "/Users/li/Downloads/traningset/LYFW.txt"
    getTrainingSet(urlCatagory, urlContent, catagory17, datafile17)
    println("第17个文件保存完毕--------")

    val catagory18 = "电力"
    val datafile18 = "/Users/li/Downloads/traningset/DL.txt"
    getTrainingSet(urlCatagory, urlContent, catagory18, datafile18)
    println("第18个文件保存完毕--------")

    val catagory19 = "电子信息"
    val datafile19 = "/Users/li/Downloads/traningset/DZXX.txt"
    getTrainingSet(urlCatagory, urlContent, catagory19, datafile19)
    println("第19个文件保存完毕--------")

    val catagory20 = "交通设施"
    val datafile20 = "/Users/li/Downloads/traningset/JTSS.txt"
    getTrainingSet(urlCatagory, urlContent, catagory20, datafile20)
    println("第20个文件保存完毕--------")

    val catagory21 = "工程建筑"
    val datafile21 = "/Users/li/Downloads/traningset/GCJZ.txt"
    getTrainingSet(urlCatagory, urlContent, catagory21, datafile21)
    println("第21个文件保存完毕--------")

    val catagory22 = "农林牧渔"
    val datafile22 = "/Users/li/Downloads/traningset/NLMY.txt"
    getTrainingSet(urlCatagory, urlContent, catagory22, datafile22)
    println("第22个文件保存完毕--------")

    val catagory23 = "造纸印刷"
    val datafile23 = "/Users/li/Downloads/traningset/ZZYS.txt"
    getTrainingSet(urlCatagory, urlContent, catagory23, datafile23)
    println("第23个文件保存完毕--------")

    val catagory24 = "计算机"
    val datafile24 = "/Users/li/Downloads/traningset/JSJ.txt"
    getTrainingSet(urlCatagory, urlContent, catagory24, datafile24)
    println("第24个文件保存完毕--------")

    val catagory25 = "有色金属"
    val datafile25 = "/Users/li/Downloads/traningset/YSJS.txt"
    getTrainingSet(urlCatagory, urlContent, catagory25, datafile25)
    println("第25个文件保存完毕--------")

    val catagory26 = "保险"
    val datafile26 = "/Users/li/Downloads/traningset/BX.txt"
    getTrainingSet(urlCatagory, urlContent, catagory26, datafile26)
    println("第26个文件保存完毕--------")

    val catagory27 = "券商"
    val datafile27 = "/Users/li/Downloads/traningset/QS.txt"
    getTrainingSet(urlCatagory, urlContent, catagory27, datafile27)
    println("第27个文件保存完毕--------")

    val catagory28 = "其他行业"
    val datafile28 = "/Users/li/Downloads/traningset/QTHY.txt"
    getTrainingSet(urlCatagory, urlContent, catagory28, datafile28)
    println("第28个文件保存完毕--------")

    val catagory29 = "银行类"
    val datafile29 = "/Users/li/Downloads/traningset/YHL.txt"
    getTrainingSet(urlCatagory, urlContent, catagory29, datafile29)
    println("第29个文件保存完毕--------")

    val catagory30 = "非银金融"
    val datafile30 = "/Users/li/Downloads/traningset/FYJR.txt"
    getTrainingSet(urlCatagory, urlContent, catagory30, datafile30)
    println("第30个文件保存完毕--------")

    val catagory31 = "教育传媒"
    val datafile31 = "/Users/li/Downloads/traningset/JYCM.txt"
    getTrainingSet(urlCatagory, urlContent, catagory31, datafile31)
    println("第31个文件保存完毕--------")

    val catagory32 = "外贸"
    val datafile32 = "/Users/li/Downloads/traningset/WM.txt"
    getTrainingSet(urlCatagory, urlContent, catagory32, datafile32)
    println("第32个文件保存完毕--------")

  }

  //  val


}
