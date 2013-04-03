package edu.berkeley.blbspark

import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.scalatest.matchers.ShouldMatchers
import spark.SparkContext
import java.util.Random
import util.Statistics

class StratifiedBlbSuite extends FunSuite with BeforeAndAfterEach with ShouldMatchers {
  var sc: SparkContext = _
  val defaultNumSplits = 4
  val seed = 1234L

  override def beforeEach {
    sc = new SparkContext("local", "test")
  }

  override def afterEach {
    sc.stop()
    sc = null
    System.clearProperty("spark.master.port")
  }

  test("stratified BLB with only small strata should give 0 error") {
    val stratumSize = 100
    val numStrata = 100
    // We simulate that the stratified sample has the first 2000 values from
    // each stratum.  The strata here have only stratumSize values, so BLB
    // should treat them all as deterministic.
    val stratifiedSampleCap = 2000
    val strata = (0 until numStrata)
    val random = new Random(seed)

    val data = strata.flatMap(stratum => (0 until stratumSize).map((sampleIdx: Int) => {
      WeightedItem(Datum(stratum, random.nextDouble()), 1.0)}))
    val error = StratifiedBlb.stratifiedBlb(
    sc.parallelize(data, defaultNumSplits),
      {datum: WeightedItem[Datum] => datum.item.stratum},
      StratifiedBlbSuite.weightedMean,
      Statistics.sampleStdDev,
      Statistics.mean,
      0.7,
      5,
      100,
      stratifiedSampleCap,
      defaultNumSplits,
      random.nextInt()
    )
    error should be (0)
  }

  test("stratified BLB with a single large stratum should produce the usual bootstrap estimate") {
    val stratumSize = 1000
    val numStrata = 1
    val stratifiedSampleCap = 1000
    val strata = (0 until numStrata)
    val random = new Random(seed)

    // We simulate a sample from 2000 elements; each sampled element gets
    // weight 2.
    val data = strata.flatMap(stratum => (0 until stratumSize).map((sampleIdx: Int) => {
      WeightedItem(Datum(stratum, random.nextDouble()), 2.0)}))
    val error = StratifiedBlb.stratifiedBlb(
      sc.parallelize(data, defaultNumSplits),
      {datum: WeightedItem[Datum] => datum.item.stratum},
      StratifiedBlbSuite.weightedMean,
      Statistics.sampleStdDev,
      Statistics.mean,
      0.7,
      5,
      100,
      stratifiedSampleCap,
      defaultNumSplits,
      random.nextInt()
    )
    error should be (0.0091 plusOrMinus(0.002))
  }

  test("stratified BLB with many small strata and one large stratum should reflect 0 error in the small strata" +
    " and some error in the large stratum") {
    val smallStratumSize = 100
    val largeStratumSize = 1000
    val numSmallStrata = 100
    val numLargeStrata = 1
    val stratifiedSampleCap = 1000
    val random = new Random(seed)

    val data = (0 until numSmallStrata).flatMap(stratum => (0 until smallStratumSize).map((sampleIdx: Int) => {
      WeightedItem(Datum(stratum, random.nextDouble()), 1.0)}))
      .union((0 until numLargeStrata).flatMap(stratum => (0 until largeStratumSize).map((sampleIdx: Int) => {
      WeightedItem(Datum(stratum, random.nextDouble()), 2.0)})))
    val error = StratifiedBlb.stratifiedBlb(
      sc.parallelize(data, defaultNumSplits),
      {datum: WeightedItem[Datum] => datum.item.stratum},
      StratifiedBlbSuite.weightedMean,
      Statistics.sampleStdDev,
      Statistics.mean,
      0.7,
      5,
      100,
      stratifiedSampleCap,
      defaultNumSplits,
      random.nextInt()
    )
    error should be (0.0015 plusOrMinus(0.001))
  }

  test("stratified BLB with many small strata and several large strata produces appropriate error estimates") {
    val smallStratumSize = 100
    val largeStratumSize = 1000
    val numSmallStrata = 100
    val numLargeStrata = 5
    val stratifiedSampleCap = 1000
    val random = new Random(seed)

    // Stratum i has data uniform on [0, i] and has original (pre-sample)
    // size i*1000
    val data = (0 until numSmallStrata).flatMap(stratum => (0 until smallStratumSize).map((sampleIdx: Int) => {
      WeightedItem(Datum(stratum, random.nextDouble()), 1.0)}))
      .union((0 until numLargeStrata).flatMap(stratum => (0 until largeStratumSize).map((sampleIdx: Int) => {
      WeightedItem(Datum(stratum, random.nextDouble()*(stratum+1)), stratum+1)})))
    val error = StratifiedBlb.stratifiedBlb(
      sc.parallelize(data, defaultNumSplits),
      {datum: WeightedItem[Datum] => datum.item.stratum},
      StratifiedBlbSuite.weightedMean,
      Statistics.sampleStdDev,
      Statistics.mean,
      0.7,
      5,
      100,
      stratifiedSampleCap,
      defaultNumSplits,
      random.nextInt()
    )
    error should be (0.015 plusOrMinus(0.01))
  }

  test("stratified blb still produces correct answers when the data are preshuffled randomly") {
    // The setup for this test is identical to that of the previous test,
    // except for the shuffling.
    val smallStratumSize = 100
    val largeStratumSize = 1000
    val numSmallStrata = 100
    val numLargeStrata = 5
    val stratifiedSampleCap = 1000
    val random = new Random(seed)

    val data = (0 until numSmallStrata).flatMap(stratum => (0 until smallStratumSize).map((sampleIdx: Int) => {
      WeightedItem(Datum(stratum, random.nextDouble()), 1.0)}))
      .union((0 until numLargeStrata).flatMap(stratum => (0 until largeStratumSize).map((sampleIdx: Int) => {
      WeightedItem(Datum(stratum, random.nextDouble()*(stratum+1)), stratum+1)})))
    val shuffledData = new scala.util.Random(seed).shuffle(data)
    val error = StratifiedBlb.stratifiedBlb(
      sc.parallelize(shuffledData, defaultNumSplits),
      {datum: WeightedItem[Datum] => datum.item.stratum},
      StratifiedBlbSuite.weightedMean,
      Statistics.sampleStdDev,
      Statistics.mean,
      0.7,
      5,
      100,
      stratifiedSampleCap,
      defaultNumSplits,
      random.nextInt()
    )
    error should be (0.015 plusOrMinus(0.01))
  }

  test("stratified blb produces similar answers for black-box and commutative monoid operators") {
    //FIXME
  }
}

object StratifiedBlbSuite {
  def weightedMean = StratifiedBlb.CommutativeMonoidRddOperator(
    Aggregate(0.0, 0.0),
    {(agg: Aggregate, datum: WeightedItem[Datum]) => Aggregate(agg.count+datum.weight, agg.sum + datum.weight*datum.item.value)},
    {(agg1: Aggregate, agg2: Aggregate) => Aggregate(agg1.count+agg2.count, agg1.sum+agg2.sum)},
    {(agg: Aggregate) => agg.sum / agg.count})
}

case class Datum(stratum: Int, value: Double)
case class Aggregate(count: Double, sum: Double)