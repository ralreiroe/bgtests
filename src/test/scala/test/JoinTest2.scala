package test

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}

class JoinTest2 extends FlatSpec with Matchers {

  "a" should "b" in {

    val conf = new SparkConf().setAppName("SOME APP NAME").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)

    val sparkSession: SparkSession = SparkSession.builder().appName("aaa").getOrCreate()
    import sparkSession.implicits._


    val langPercentDF: DataFrame = sparkSession.createDataFrame(List(("Scala", 35), ("Python", 30), ("R", 15), ("Java", 20)))

    langPercentDF.show()


  }

}
