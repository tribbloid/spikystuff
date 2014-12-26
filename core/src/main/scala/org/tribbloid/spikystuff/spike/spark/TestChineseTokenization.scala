package org.tribbloid.spikystuff.spike.spark

import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.{SparkContext, SparkConf}
import org.tribbloid.spikystuff.spike.tokenization.NlpirMethod

/**
 * Created by peng on 10/16/14.
 */
object TestChineseTokenization {

  val hashingTF = new HashingTF()
  import SparkContext._

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Spark Pi")
    conf.setMaster("local[*]")
    conf.setSparkHome(System.getenv("SPARK_HOME"))
    val sc = new SparkContext(conf)

        val raw = sc.textFile("file:///home/peng/Grive/anchorbot/Extracted Data/大众点评.tsv")
          .map{
          row => {
            val splitted = row.split("\t")
            (splitted(3),splitted(12))
          }
        }

//    val raw = sc.textFile("file:///home/peng/Grive/anchorbot/Extracted Data/pocofood_noheader.tsv")
//      .map{
//      row => {
//        val splitted = row.split("\t")
//        (splitted(2),splitted(10))
//      }
//    }

    val aggreg = raw.reduceByKey(_ + " " + _)

//    aggreg.persist().collect().foreach(println)

    NlpirMethod.Nlpir_init

    val tokenized = aggreg.mapValues(text => NlpirMethod.NLPIR_ParagraphProcess(text, 1).split(" ").toSeq)

//    tokenized.persist().collect().foreach(println)

    val tf = hashingTF.transform(tokenized.values).persist()
    val idf = new IDF().fit(tf)
    val tfidf = idf.transform(tf)

    //    val lookup = tokenized.mapValues(tokens => (tokens, tokens.map(hashingTF.indexOf(_))))

    val all = tokenized.zip(tfidf).map(tuple => (tuple._1._1, tuple._1._2.distinct, tuple._2))

    val wordvec = all.map(
      tuple => (tuple._1, tuple._2.map(
        word =>(word, tuple._3(hashingTF.indexOf(word)))
      ).sortBy(- _._2))
    ).mapValues(_.mkString(",")).map(tuple => tuple._1 + "\t"+ tuple._2).persist()

        wordvec.saveAsTextFile("file:///home/peng/Grive/anchorbot/Extracted Data/大众点评_word")

//    wordvec.saveAsTextFile("file:///home/peng/Grive/anchorbot/Extracted Data/pocofood_word")

    val result = wordvec.collect()

    result.foreach(println)

    sc.stop()
    //    data.split(" ").foreach(println)
  }
}
