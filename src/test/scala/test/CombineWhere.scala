package test

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.scalatest._

import scala.collection.mutable.ArrayBuffer


/**
  *
  * GroupByKey = no combiner before shuffle
  * But if the combiner does not reduce much it does not matter
  *
  * https://databricks.gitbooks.io/databricks-spark-knowledge-base/content/best_practices/prefer_reducebykey_over_groupbykey.html
  *
  *
  * How *not* to optimize
  * https://github.com/awesome-spark/spark-gotchas/blob/master/04_rdd_actions_and_transformations_by_example.md#rdd-actions-and-transformations-by-example
  *
  */
class CombineWhere extends FlatSpec with Matchers {
  "Hello" should "have tests" in {

    val conf = new SparkConf().setAppName("SOME APP NAME").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val rdd: RDD[(Int, String)] = sc.parallelize(Seq((1, "foo"), (1, "bar"), (2, "foobar")), 2)

    val x: RDD[(Int, Iterable[String])] = rdd.groupByKey()

    //groupByKey is equivalent to

    val x2 = rdd.combineByKeyWithClassTag[ArrayBuffer[String]](
      ArrayBuffer(_: String),
      (acc: ArrayBuffer[String], x: String) => acc += x,
      (acc1: ArrayBuffer[String], acc2: ArrayBuffer[String]) => acc1 ++= acc2, new HashPartitioner(2), mapSideCombine = false, null)

    val xc = x2.collect()

    println(xc.toList)

    //...all tuples are shuffled by key before the combiner runs



    //is a reduceByKey(f) style transformation, where the combiner f is executed before the shuffle, better?

    //the following

    val x3 = rdd.combineByKey(
      (x: String) => ArrayBuffer(x),
      (acc: ArrayBuffer[String], x: String) => acc += x,
      (acc1: ArrayBuffer[String], acc2: ArrayBuffer[String]) => acc1 ++= acc2
    )

    println(x3.collect.toList)


    //is equivalent to

    val x4 = rdd.combineByKeyWithClassTag[ArrayBuffer[String]](
      ArrayBuffer(_: String),
      (acc: ArrayBuffer[String], x: String) => acc += x,
      (acc1: ArrayBuffer[String], acc2: ArrayBuffer[String]) => acc1 ++= acc2, new HashPartitioner(2), mapSideCombine = true, null)

    //...so the combiner runs on the partitions *before* the shuffle occurs
    // but because the combiner does not reduce the amount of data it does not matter    <======

    println(x4.collect.toList)


    //but with the following combiner, we have a huge reduction and it pays off doing it before the shuffle

    val x5 = rdd.combineByKeyWithClassTag[Int](
      (s: String) => s.length,
      (acc: Int, x: String) => acc + x.length,
      (acc1: Int, acc2: Int) => acc1 + acc2, new HashPartitioner(2), mapSideCombine = true, null)


    println(x5.collect.toList)




  }

}




