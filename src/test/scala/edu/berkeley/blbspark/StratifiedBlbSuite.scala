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

    test("stratified blb with all strata small") {
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
        StratifiedBlbSuite.mean,
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

    test("stratified blb with a single large stratum") {
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
        StratifiedBlbSuite.mean,
        Statistics.sampleStdDev,
        Statistics.mean,
        0.7,
        5,
        100,
        stratifiedSampleCap,
        defaultNumSplits,
        random.nextInt()
      )
      // This is just a back-of-the-envelope calculation.
      error should be (0.0075 plusOrMinus(.001))
    }

    test("stratified blb with many small strata and one large stratum") {
      val smallStratumSize = 100
      val largeStratumSize = 1000
      val numStrata = 101
      val stratifiedSampleCap = 1000
      val strata = (0 until numStrata)
      val random = new Random(seed)

      val data = strata.flatMap(stratum => (0 until stratumSize).map((sampleIdx: Int) => {
        WeightedItem(Datum(stratum, random.nextDouble()), if (sampleIdx >= 1.0)}))
      val error = StratifiedBlb.stratifiedBlb(
      sc.parallelize(data, defaultNumSplits),
        {datum: WeightedItem[Datum] => datum.item.stratum},
        StratifiedBlbSuite.mean,
        Statistics.sampleStdDev,
        Statistics.mean,
        0.7,
        5,
        100,
        stratifiedSampleCap,
        defaultNumSplits,
        random.nextInt()
      )
    }

    test("stratified blb with many small strata and several large stratum") {
      //FIXME
    }
}

object StratifiedBlbSuite {
  val mean = StratifiedBlb.CommutativeMonoidRddOperator(
    Aggregate(0.0, 0.0),
    {(agg: Aggregate, datum: WeightedItem[Datum]) => Aggregate(agg.count+datum.weight, agg.sum + datum.weight*datum.item.value)},
    {(agg1: Aggregate, agg2: Aggregate) => Aggregate(agg1.count+agg2.count, agg1.sum+agg2.sum)},
    {(agg: Aggregate) => agg.sum / agg.count})
}

case class Datum(stratum: Int, value: Double)
case class Aggregate(count: Double, sum: Double)