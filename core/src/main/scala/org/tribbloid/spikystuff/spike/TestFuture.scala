package org.tribbloid.spikystuff.spike

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Created by peng on 10/4/14.
 */
object TestFuture {

  def main(args: Array[String]) {

    val fu = Future {
      for (i <- 1 to 10) {
        println("received")
        Thread.sleep(100)
      }
    }

    Await.result(fu, 15 seconds)

    println("finished")
  }
}
