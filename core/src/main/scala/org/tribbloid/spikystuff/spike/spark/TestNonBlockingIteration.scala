package org.tribbloid.spikystuff.spike.spark

import org.apache.spark.SparkContext.IntAccumulatorParam
import org.apache.spark.{SparkConf, SparkContext}

import scala.concurrent.{Await, Future}

object TestNonBlockingIteration {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("TestNonBlockingIteration")
    //    conf.setMaster("local-cluster[2,4,1000]") //no can do! spark cannot find jars
    conf.setMaster("local[8,3]")
    conf.setSparkHome(System.getenv("SPARK_HOME"))
    val sc = new SparkContext(conf)

    val input = sc.parallelize(1 to 10)
    
    val acc = sc.accumulator(-1)(IntAccumulatorParam)

    import scala.concurrent.ExecutionContext.Implicits.global
    import scala.concurrent.duration._

    val future = Future {
      for (i <- 1 to 10) {
        sc.broadcast(AutoInsert(i))
        Thread.sleep(1000)
        println(i)
      }
    }

    future.onSuccess{case u => println("success")}

    val rdd1 = input.foreachPartition{
      values => {
        for (i<- 1 to 100) {
          acc += WorkerContainer.last
          println("feeding: " + WorkerContainer.last)
          Thread.sleep(100)
        }
      }
    }

    Await.result(future, 100 seconds)
    println(acc.value)

    sc.stop()
    println("finished")
  }

}

object TestNonBlockingIterationSubmit {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("TestNonBlockingIteration")
    //    conf.setMaster("local-cluster[2,4,1000]") //no can do! spark cannot find jars
    val sc = new SparkContext(conf)

    val input = sc.parallelize(1 to 10)

    val acc = sc.accumulator(-1)(IntAccumulatorParam)

    import scala.concurrent.ExecutionContext.Implicits.global
    import scala.concurrent.duration._

    val future = Future {
      for (i <- 1 to 10) {
        sc.broadcast(AutoInsert(i))
        Thread.sleep(1000)
        println(i)
      }
    }

    future.onSuccess{case u => println("success")}

    val rdd1 = input.foreachPartition{
      values => {
        for (i<- 1 to 100) {
          acc += WorkerContainer.last
          println("feeding: " + WorkerContainer.last)
          Thread.sleep(100)
        }
      }
    }

    Await.result(future, 100 seconds)
    println(acc.value)

    sc.stop()
    println("finished")
  }

}
