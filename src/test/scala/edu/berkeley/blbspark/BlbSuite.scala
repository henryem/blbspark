package edu.berkeley.blbspark

import org.scalatest.{BeforeAndAfterEach, FunSuite}
import spark.SparkContext
import org.scalatest.matchers.ShouldMatchers

//FIXME
class BlbSuite extends FunSuite with BeforeAndAfterEach with ShouldMatchers {
  var sc: SparkContext = _

  override def beforeEach {
    sc = new SparkContext("local", "test")
  }

  override def afterEach {
    sc.stop()
    sc = null
    System.clearProperty("spark.master.port")
  }

  test("subsampling") {
    //FIXME
  }

  test("resampling") {
    //FIXME
  }

  test("blb on a simple stddev of a mean") {
    val numZeroes = 100
    val numTens = 900
    val totalItems = numZeroes + numTens
    val proportionZeroes = numZeroes.toDouble / totalItems
    val proportionTens = numTens.toDouble / totalItems
    val testData = sc.parallelize(
      (Iterator.fill(numZeroes)(0.0) ++ Iterator.fill(numTens)(10.0))
        .map(int => WeightedItem.single(int))
        .toSeq,
      20)
    val mean = (items: Seq[WeightedItem[Double]]) => items.map(_.item).sum / items.size
    val sampleStdDev = (thetas: Seq[Double]) => {
      val squares = thetas.map(thetaVal => thetaVal*thetaVal)
      val sumSquares = squares.sum
      val squaredSum = math.pow(thetas.sum, 2)
      math.pow((sumSquares - squaredSum / thetas.size) / (thetas.size - 1), 0.5)
    }
    val averageSampleStdDevs = (sampleStdDevs: Seq[Double]) => sampleStdDevs.sum / sampleStdDevs.size

    val trueStdDev = 10.0*math.pow(proportionTens * (1 - proportionTens) / totalItems, 0.5)

    val alpha = 0.7
    val s = math.pow(totalItems, 1 - alpha).toInt
    val r = 10
    val blbDistributedSamples = Blb.resample(Blb.makeBlbSubsamples(testData, totalItems, alpha, s, 10), r, totalItems)
    val blbEstimatedStdDev = Blb.blb(blbDistributedSamples, mean, sampleStdDev, averageSampleStdDevs)

    blbEstimatedStdDev should be (trueStdDev plusOrMinus (trueStdDev / 10.0))
  }
}
