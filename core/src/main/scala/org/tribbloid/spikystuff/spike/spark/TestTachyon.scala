package org.tribbloid.spikystuff.spike.spark

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by peng on 12/21/14.
 */
object TestTachyon {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("TestBroadcastUpdate")
      .setMaster("local[8,3]")

    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(1 to 100).keyBy(_ % 10)

    rdd.persist(StorageLevel.OFF_HEAP)

    println(rdd.count())

    rdd.collect().foreach(println)
  }
}
