package org.tribbloid.spikystuff.spike.spark

import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}
import SparkContext._

/**
 * Created by peng on 1/20/15.
 */
object TestPairPartitioning extends Submittable {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TestBroadcastUpdate")
      .setMaster("local[8,3]")

    val sc = new SparkContext(conf)

    import SparkContext._

    val rdd = sc.parallelize(1 to 100).keyBy(v => 1).partitionBy(new HashPartitioner(20))//.partitionBy(new HashPartitioner(10))

    rdd.foreachPartition(v => println(v.size))
    //
    //  rdd.collect().foreach(println)
      println(rdd.partitioner)

    sc.stop()
  }
}
