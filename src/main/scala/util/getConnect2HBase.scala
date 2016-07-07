package util

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by li on 16/7/7.
  */
object getConnect2HBase {


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("getConnect2HBase").setMaster("local")
    val sc = new SparkContext()

    // 初始化配置
    val configuration = new HBaseConfiguration.create()
    configuration.set("hbase.zookeeper.property.cilentport", "2181") //设置zookeeper client 端口
      .set("hbase.zookeeper.quorum", "localhost") //设置zookeeper quorum
      .set("hbasemaster", "localhost:60000") //设置hbase master
      .addResource() //将hbase的配置加载

    //实例化Hbase
    val hadmin = new HBaseAdmin(configuration)

    // 使用Hadoop api来创建一个RDD
    val hrdd = sc.newAPIHadoopRDD(configuration, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

  }

}
