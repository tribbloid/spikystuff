package org.tribbloid.spikystuff.spike.spark

import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}

/**
 * Created by peng on 12/20/14.
 */
object TestPartitioner {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TestBroadcastUpdate")
      .setMaster("local[8,3]")

    val sc = new SparkContext(conf)

    import SparkContext._

    val rdd = sc.parallelize(1 to 100).keyBy(_ % 10)//.partitionBy(new HashPartitioner(10))

    rdd.collect().foreach(println)
    println(rdd.partitioner)

    sc.stop()
  }
}
