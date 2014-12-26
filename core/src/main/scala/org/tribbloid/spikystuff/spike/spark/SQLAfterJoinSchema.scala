package org.tribbloid.spikystuff.spike.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by peng on 11/26/14.
 */

object SQLAfterJoinSchema {

  case class AA(
                 key: Int,
                 name: String
                 )

  def main(args: Array[String]) {


    val conf = new SparkConf().setAppName("TestBroadcastUpdate")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    val sql = new SQLContext(sc)

    import sql._

    val first = sc.parallelize(
      AA(1,"T1") ::
        AA(2,"T2") ::
        AA(1,"T3") ::Nil
    ).as('first)

    val last = sc.parallelize(
      AA(1,"alpha") ::
        AA(2,"beta") ::Nil
    ).as('last)

    val joined = first.join(last.toSchemaRDD, joinType = LeftOuter, on = Some("first.key".attr === "last.key".expr))

    joined.map(row => row).collect().foreach(println)
  }
}