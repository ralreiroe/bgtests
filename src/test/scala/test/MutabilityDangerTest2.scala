package test

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

import scala.collection.mutable.ArrayBuffer

class MutabilityDangerTest2 extends FlatSpec with Matchers {
  "Hello" should "have tests" in {

    val seq = Seq(ArrayBuffer(1, 1), ArrayBuffer(1, 1))

    val conf = new SparkConf().setAppName("SOME APP NAME").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)

//    val r: ArrayBuffer[Int] = seq.reduce(_++=_)       //tail.foldLeft[B](head)(f)
//    println(r)
//    println(seq)

    val rdd = sc.parallelize(seq,1)
    rdd.cache()
    val r2: ArrayBuffer[Int] = rdd.fold(ArrayBuffer.empty[Int])(_++=_)
    println("++++seq: "+seq)
    println("++++r2: "+r2)
    val r3: ArrayBuffer[Int] = rdd.fold(ArrayBuffer.empty[Int])(_++=_)
    println("++++seq: "+seq)
    println("++++r3: "+r3)
    val r4: ArrayBuffer[Int] = rdd.fold(ArrayBuffer.empty[Int])(_++=_)
    println("++++seq: "+seq)
    println("++++r4: "+r4)
  }

}




