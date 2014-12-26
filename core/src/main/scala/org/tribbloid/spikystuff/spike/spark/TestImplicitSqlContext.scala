package org.tribbloid.spikystuff.spike.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by peng on 12/20/14.
 */
object TestImplicitSqlContext {

  def toSchema(implicit sql: SQLContext): String = {
    sql.toString
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("TestBroadcastUpdate")
      .setMaster("local[8,3]")

    val sc = new SparkContext(conf)
    implicit val sql = new SQLContext(sc)

    import sql._

    this.toSchema
  }
}
