package org.tribbloid.spikystuff.spike.spark

import java.io.ObjectInputStream

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by peng on 10/1/14.
 */
object WorkerContainer {
  @volatile var last: Int = -1
}

class AutoInsert(var value: Int) extends Serializable{

  WorkerContainer.last = value

  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    WorkerContainer.last = this.value
  }
}

object TestBroadcastUpdate {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("TestBroadcastUpdate")
      .setMaster("local[8,3]")
//      .setMaster("local-cluster[2, 1, 1024]") //no can do! spark cannot find jars
//      .set("spark.akka.frameSize", "1") // set to 1MB to detect direct serialization of data

//    conf.setSparkHome(System.getenv("SPARK_HOME"))
    val sc = new SparkContext(conf)

    val input = sc.parallelize(1 to 10, 8)

    sc.broadcast(new AutoInsert(2))

    val rdd1 = input.map(_ + WorkerContainer.last).persist()

    rdd1.count()

    sc.broadcast(new AutoInsert(3))

    val rdd2 = rdd1.map(_ + WorkerContainer.last)

    rdd2.collect().foreach(println)

//    println("Pi is roughly " + 4.0 * count / n)
    sc.stop()
    println("finished")
  }
}

object TestBroadcastUpdateSubmit {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("TestBroadcastUpdate")
    //    conf.setMaster("local-cluster[2,4,1000]") //no can do! spark cannot find jars
    val sc = new SparkContext(conf)

    val input = sc.parallelize(1 to 10, 8)

    sc.broadcast(new AutoInsert(2))

    val rdd1 = input.map(_ + WorkerContainer.last).persist()

    rdd1.count()

    sc.broadcast(new AutoInsert(3))

    val rdd2 = rdd1.map(_ + WorkerContainer.last)

    rdd2.collect().foreach(println)

    //    println("Pi is roughly " + 4.0 * count / n)
    sc.stop()
    println("finished")
  }
}
