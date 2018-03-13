package test

import java.io.InputStream

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

import scala.io.Source

class WordcountTest extends FlatSpec with Matchers {
  "Hello" should "have tests" in {

    val is: InputStream = getClass.getResourceAsStream("wordcountTest-regimeonly.txt")

//    val fileLines: Seq[String] = Source.fromFile("/i/p/ralfoenning/bigdata/src/test/scala/test/wordcountTest-regimeonly.txt").getLines.toList

    val fileLines: Seq[String] = Source.fromInputStream(is).getLines.toList

    val res: Map[String, Int] = fileLines.groupBy(identity).mapValues(_.size)

    println(res)
    println(fileLines.groupBy(identity))

    val conf = new SparkConf().setAppName("SOME APP NAME").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
    val ret: RDD[String] = sc.textFile("/i/p/ralfoenning/bigdata/src/test/scala/test/wordcountTest-regimeonly.txt")

    ret.groupBy(identity).foreach(println(_))

    ret.map((_,1)).groupByKey().foreach(println(_))

//      mapValues(_.sum)    //(LADP,1)...

  }

}




