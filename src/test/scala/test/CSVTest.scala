package test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}

class CSVTest extends FlatSpec with Matchers {

  "a" should "b" in {

    val conf = new SparkConf().setAppName("SOME APP NAME").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)


    // Read the CSV file
    val csv: RDD[String] = sc.textFile("/i/p/ralfoenning/bigdata/src/test/scala/test/csv.txt")
    // split / clean data
    val headerAndRows: RDD[Array[String]] = csv.map(line => line.split(",").map(_.trim))
    // get header
    val header: Array[String] = headerAndRows.first
    // filter out header (eh. just check if the first val matches the first header name)
    val data: RDD[Array[String]] = headerAndRows.filter(_(0) != header(0))
    // splits to map (header/value pairs)
    val maps = data.map(strings => header.zip(strings).toMap)
    // filter out the user "me"
    val result = maps.filter(map => map("user") != "me")
    // print result
    result.foreach(println)


  }

}
