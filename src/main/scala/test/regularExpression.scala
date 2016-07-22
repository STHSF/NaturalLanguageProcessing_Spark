package test

/**
  * Created by li on 16/7/22.
  */
object regularExpression {

  def main(args: Array[String]) {

    val numPatten = """([0-9]+) ([a-z]+\s+)""".r

//    val numPatten = """(\s+[0-9]+\s+) ([0-9]+) ()""".r

    val res = numPatten.findAllIn("99 bottles, 89 bottles").toArray

    res.foreach(println)

  }

}
