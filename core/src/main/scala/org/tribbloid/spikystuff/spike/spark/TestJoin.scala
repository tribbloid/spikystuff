package org.tribbloid.spikystuff.spike.spark

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
 * Created by peng on 11/19/14.
 */
object TestJoin {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("TestBroadcastUpdate")
      .setMaster("local[8,3]")


    val sc = new SparkContext(conf)

    val input = sc.parallelize(1 to 100, 8).map(a => a-> a)

    val input2 = sc.parallelize((1 to 200).map(_ => Random.nextInt(100))).map(a => a-> a)

    import SparkContext._

    val joined = input.leftOuterJoin(input2)

    joined.collect().foreach(println)
  }
}
