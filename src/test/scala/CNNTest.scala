import breeze.linalg.{CSCMatrix => BSM, DenseMatrix => BDM, DenseVector => BDV, Matrix => BM, SparseVector => BSV, Vector => BV, accumulate => Accumulate, axpy => brzAxpy, rot90 => Rot90, sum => Bsum, svd => brzSvd}
import breeze.numerics.{exp => Bexp, tanh => Btanh}
import org.apache.spark.mllib.linalg.DenseMatrix


/**
  * Created by li on 16/8/15.
  */
object CNNTest {


  def main(args: Array[String]) {

    def sigm(matrix: BDM[Double]): BDM[Double] = {
      val s1 = 1.0 / (Bexp(matrix * (-1.0)) + 1.0)
      s1
    }

    val result = BDM.ones[Double](2, 3) + 1.8

    println(result)


    val re = sigm(result)

    println(re)


  }

}
