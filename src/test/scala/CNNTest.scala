import breeze.linalg.DenseMatrix
import breeze.numerics._

import breeze.linalg.{ Matrix => BM, CSCMatrix => BSM,
DenseMatrix => BDM, Vector => BV,
DenseVector => BDV, SparseVector => BSV, axpy => brzAxpy,
svd => brzSvd, accumulate => Accumulate, rot90 => Rot90, sum => Bsum
}

import breeze.numerics.{
exp => Bexp, tanh => Btanh
}


/**
  * Created by li on 16/8/15.
  */
object CNNTest {


  def main(args: Array[String]) {

    val dd = BDM.ones[Double](3, 3)
    val jj = BDM.ones[Double](3, 3)

    def sigm(matrix: BDM[Double]): BDM[Double] = {
      val s1 = 1.0 / (Bexp(matrix * (-1.0)) + 1.0)
      s1
    }

    val j = sigm(dd)
    println(j)
  }

}
