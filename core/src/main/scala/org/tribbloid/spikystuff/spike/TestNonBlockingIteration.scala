package org.tribbloid.spikystuff.spike

import org.apache.spark.SparkContext.IntAccumulatorParam
import org.apache.spark.{SparkContext, SparkConf}

import scala.concurrent.{Await, Future}

object TestNonBlockingIteration {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("TestNonBlockingIteration")
    //    conf.setMaster("local-cluster[2,4,1000]") //no can do! spark cannot find jars
    conf.setMaster("local[8,3]")
    conf.setSparkHome(System.getenv("SPARK_HOME"))
    val sc = new SparkContext(conf)

    val input = sc.parallelize(1 to 10)
    
    val acc = sc.accumulator(0)(IntAccumulatorParam)

    import scala.concurrent.ExecutionContext.Implicits.global

    val future = Future {
      for (i <- 1 to 10) {
        sc.broadcast(AutoInsert(i))
        wait(1000)
        println(i)
      }
    }

    future.onSuccess{case u => println("success")}

//    val rdd1 = input.foreachPartition{
//      values => {
//        for (i<- 1 to 100000000) {
//          acc += WorkerContainer.last
//        }
//      }
//    }

//    println(acc.value)

    //    println("Pi is roughly " + 4.0 * count / n)
    sc.stop()
//    println("finished")
  }

}
