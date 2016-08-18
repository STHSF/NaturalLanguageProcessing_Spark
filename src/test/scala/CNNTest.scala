import breeze.linalg.{DenseMatrix, DenseVector}

//import breeze.linalg.{CSCMatrix => BSM, DenseMatrix => BDM, DenseVector => BDV, Matrix => BM, SparseVector => BSV, Vector => BV, accumulate => Accumulate, axpy => brzAxpy, rot90 => Rot90, sum => Bsum, svd => brzSvd, DenseVector}
//import breeze.numerics.{exp => Bexp, tanh => Btanh}
//import org.apache.spark.mllib.linalg.DenseMatrix


/**
  * Created by li on 16/8/15.
  */
object CNNTest {


  def main(args: Array[String]) {
//
//    def sigm(matrix: BDM[Double]): BDM[Double] = {
//      val s1 = 1.0 / (Bexp(matrix * (-1.0)) + 1.0)
//      s1
//    }
//
//    val result = BDM.ones[Double](2, 3) + 1.8



    val a = DenseVector(1.0, 2.0, 3.0, 4.0, 5.0)

    val b = DenseVector(1.0, 2.0, 3.0, 4.0, 5.0)

    val c = DenseMatrix.ones[Double](5, 2)

    val d = DenseMatrix.ones[Double](5, 5)

    println((a.toDenseMatrix :* d))


//    val c = (a :* b) :* d
//
//    println(c)



  }

}
