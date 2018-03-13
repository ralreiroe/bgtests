package test

import java.io.InputStream

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

class MutabilityDangerTest extends FlatSpec with Matchers {
  "Hello" should "have tests" in {

    val seq = Seq(ArrayBuffer(1, 1), ArrayBuffer(1, 1))

    val conf = new SparkConf().setAppName("SOME APP NAME").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)

//    val r: ArrayBuffer[Int] = seq.reduce(_++=_)       //same as tail.foldLeft[B](head)(f)
//    println(r)
//    println(seq)


    //seq never changes
    //seq is a sequence of two ArrayBuffer(1,1)
    //rdd is marked to be created on the sequence
    //rdd is marked for caching
    //rdd.reduce causes a call to parallelize and cache
    //sc.parallelize results in a partitioned Sequence. So each executor VM now holds a mini-Seq which alltogether form the full seq
    //rdd.cache causes all the mini-Seqs to be LRU-cached in each executor VM; AND every operation from here on will work on these cached mini-Seqs if not evicted
    //the reduce call will now fold via ++= all the elements but the first into the head of the first partition's mini-Seq whose head will become the concatenation of all buffers
    //the head will then be returned to the driver
    //If the altered mini-Seq in the first partition is not evicted, further operations will work on this altered mini-Seq

    //r4 and r5 will instead fold the contents of the seq into a new ArrayBuffer so no further modification of the Seq

    val rdd: RDD[ArrayBuffer[Int]] = sc.parallelize(seq,1)
    rdd.cache()
    val r2: ArrayBuffer[Int] = rdd.reduce(_++=_)
    println("++++seq: "+seq)
    println("++++r2: "+r2)
    println("++++rdd.first: "+rdd.first)
    val r3: ArrayBuffer[Int] = rdd.reduce(_++=_)
    println("++++seq: "+seq)
    println("++++r3: "+r3)
    println("++++rdd.first: "+rdd.first)
    val r4: ArrayBuffer[Int] = rdd.fold(ArrayBuffer.empty[Int])(_++=_)
    println("++++seq: "+seq)
    println("++++r4: "+r4)
    println("++++rdd.first: "+rdd.first)
    val r5: ArrayBuffer[Int] = rdd.fold(ArrayBuffer.empty[Int])(_++=_)
    println("++++seq: "+seq)
    println("++++r5: "+r5)
    println("++++rdd.first: "+rdd.first)
  }

}




