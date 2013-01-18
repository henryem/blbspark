package edu.berkeley.blbspark.rdd

import org.scalatest.{BeforeAndAfterEach, FlatSpec}
import scala.util.Random
import spark.SparkContext

class SampledRDDSuite extends FlatSpec with BeforeAndAfterEach {
  var sc: SparkContext = _
  
  override def beforeEach {
    sc = new SparkContext("local", "test")
  }
  
  override def afterEach {
    sc.stop()
    sc = null
    System.clearProperty("spark.master.port")
  }
  
  "RDD.sample(true, ...)" should "always produce the same sample when called with the same seed" in {
    val population = sc.parallelize(Seq(1, 2, 3, 4, 5))
    def takeDeterministicSample = population.sample(true, .5, 123).collect
    val firstSample = takeDeterministicSample
    for (i <- 1 to 20) {
      assert(firstSample === takeDeterministicSample)
    }
  }

  "RDD.sample(true, ...)" should "produce samples with the correct total count on average" in {
    val population = sc.parallelize(Seq(1, 2, 3, 4, 5))
    val random = new Random()
    def takeSample = population.sample(true, .5, random.nextInt).count()

  }
  
  "RDD.sample(true, ...)" should "produce a reasonable sampling distribution" in {
    val population = sc.parallelize(Seq(1, 2, 3, 4, 5))
    val random = new java.util.Random
    def takeRandomSample() = population.sample(true, .5, random.nextInt).collect
    var samples = Seq[Seq[Int]]()
    for (i <- 1 to 100) {
      samples :+= takeRandomSample.toSeq
    }
  }
}