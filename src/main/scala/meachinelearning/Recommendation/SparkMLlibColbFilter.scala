package meachinelearning.Recommendation
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by li on 2017/3/23.
  * 协同过滤ALS算法推荐过程如下：
  * 加载数据到 ratings RDD，每行记录包括：user, product, rate
  * 从 ratings 得到用户商品的数据集：(user, product)
  * 使用ALS对 ratings 进行训练
  * 通过 model 对用户商品进行预测评分：((user, product), rate)
  * 从 ratings 得到用户商品的实际评分：((user, product), rate)
  * 合并预测评分和实际评分的两个数据集，并求均方差
  */

object SparkMLlibColbFilter {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Java Collaborative Filtering Example").setMaster("local")
    val sc = new SparkContext(conf)

    // Load and parse the data
    val path = "file:///data/hadoop/spark-2.0.0-bin-hadoop2.7/data/mllib/als/test.data"
    val data = sc.textFile(path)
    val ratings = data.map(_.split(",") match { case Array(user, item, rate) =>
      Rating(user.toInt, item.toInt, rate.toDouble)
    })

    // Build the recommendation model using ALS
    val rank = 10
    val numIterations = 10
    val model = ALS.train(ratings, rank, numIterations, 0.01)

    // Evauate the model on rating data
    val usersProducts = ratings.map { case Rating(user, product, rate) =>
      (user, product)
    }

    val predictions =
      model.predict(usersProducts).map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }

    val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions)

    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = r1 - r2
      err * err
    }.mean()

    System.out.println("Mean Squared Error = " + MSE)

    // Save and load model
    model.save(sc, "target/tmp/myCollaborativeFilter")
    val sameModel = MatrixFactorizationModel.load(sc, "target/tmp/myCollaborativeFilter")


    //为每个用户进行推荐，推荐的结果可以以用户id为key，结果为value存入redis或者hbase中
    val users = data.map(_.split(",")(0)).distinct().collect()

    for (elem <- users) {

      val res = model.recommendProducts(elem.toInt, numIterations)
      res.foreach(itm => (itm.user, itm.product, itm.rating))
    }
  }
}