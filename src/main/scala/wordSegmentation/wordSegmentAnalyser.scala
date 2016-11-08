package wordSegmentation


/**
  * Created by li on 16/8/29.
  * 调用ansj分词系统
  */
object wordSegmentAnalyser {

  val content = "我是中国人,我经济南下车到广州。中国经济南下势头迅猛!"

  val candidateDic = "11, 11, 11"

  def sentenceSegment(content: String): Array[Array[String]] = {

    // 文章切分为句子
    val sentenceArr = content.split(",|。|\t|\n|，|：")
    // 句子分词
    val segResult = sentenceArr.map(AnsjAnalyzer.cutNoTag)

    segResult.foreach(x => {

      x.foreach(x => print(x + "| "))
      println()
    })

    segResult
  }


//  def isElem(sentence: Array[String], candidate: Array[String]): Boolean = {
//
//    candidate.map{ line => {
//
//      if(sentence.contains(line)) {
//
//        return true
//
//      } else {
//
//        return false
//      }
//    }}
//
//  }
//
//  def identify(sentenceSeg: Array[Array[String]],
//               candidateDic: (String, Array[String])): Array[(Array[String], Array[String])] = {
//
//    sentenceSeg.map{line => {
//      if (isElem(line, candidateDic._2)){
//
//        (line, candidateDic._1)
//      } else {
//        (line, "0")
//      }
//    }}
//
//  }


  def main(args: Array[String]) {

    //每个句子分词

    sentenceSegment(content)

    //匹配窗口设定


    //名词提出



  }

}
