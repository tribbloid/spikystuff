package org.tribbloid.spikystuff.spike

import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by peng on 10/1/14.
 */
object WorkerContainer {
  @volatile var last: Int = -1
}

case class AutoInsert(value: Int) extends Serializable{

  WorkerContainer.last = value
}

object TestBroadcastUpdate {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("TestBroadcastUpdate")
    //    conf.setMaster("local-cluster[2,4,1000]") //no can do! spark cannot find jars
    conf.setMaster("local[8,3]")
    conf.setSparkHome(System.getenv("SPARK_HOME"))
    val sc = new SparkContext(conf)

    val input = sc.parallelize(1 to 10, 8)

    sc.broadcast(AutoInsert(2))

    val rdd1 = input.map(_ * WorkerContainer.last).persist()

    rdd1.count()

    sc.broadcast(AutoInsert(3))

    val rdd2 = rdd1.map(_ * WorkerContainer.last)

    rdd2.collect().foreach(println)

//    println("Pi is roughly " + 4.0 * count / n)
    sc.stop()
    println("finished")
  }
}
