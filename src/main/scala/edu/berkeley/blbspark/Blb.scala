package edu.berkeley.blbspark

import dist.MultinomialDistribution
import edu.berkeley.blbspark.util.WeightedRepeatingIterable
import rdd.PartitionLabeledItem
import scala.util.Random
import spark.{SparkContext, RDD}

object Blb {
  /**
   * @param originalData is the data to be sampled.
   * @param n is the total weight of elements of @originalData.  For example, it can
   *   be calculated with:
   *     originalData.cache()
   *     val n = originalData.map(_.weight).fold(0.0)(_ + _).asInstanceOf[Int]
   * @param alpha determines the size of each subsample: @n^@alpha
   * @param s is the number of subsamples taken.  An important constraint is that
   *   @s * @n^@alpha <= @n.  This is necessary because subsampling is done
   *   without replacement, so only @n total things can be sampled.
   * @return a list of size @s containing RDDs of size @n^@alpha.
   */
  //TODO: @n could be allowed to be a Double instead.
  def makeBlbSubsamples[D: ClassManifest](originalData: RDD[WeightedItem[D]], n: Int, alpha: Double, s: Int, numSplits: Int): RDD[Seq[WeightedItem[D]]] = {
    val x = originalData.collect()
    val subsampleSize = math.round(math.pow(n, alpha)).asInstanceOf[Int]
    val seed = new Random().nextInt()
    val flatSamples: RDD[PartitionLabeledItem[WeightedItem[D]]] = ExactSampling.sampleRepeatedlyWithWeights(
      originalData,
      false,
      Array.fill(s)(subsampleSize),
      seed)
    val y = flatSamples.collect()
    flatSamples
      .groupBy(_.partitionLabel, numSplits)
      .map(_._2.map(_.item))
  }

  /**
   * @return an RDD of subsamples, with r resamples for each subsample.  For
   *   example, @result[0] will be a list of r resamples. @result[0][0] is a
   *   resample of total weight n.  @samples[0][0][0] is a particular weighted
   *   element from a resample.  Its weight will be an integer.
   */
  def resample[D](subsamples: RDD[Seq[WeightedItem[D]]], r: Int, n: Int): RDD[Seq[Seq[WeightedItem[D]]]] = {
    def doSingleResample(subsampleIdx: Int, subsample: Seq[WeightedItem[D]], samplingProbabilities: Seq[Double])(resampleIdx: Int): Seq[WeightedItem[D]] = {
      //NOTE: Here I am ensuring that each MultinomialDistribution across all
      // subsamples and resamples gets a unique seed.
      val counts = new MultinomialDistribution(samplingProbabilities, n, resampleIdx + r*subsampleIdx).sample()
      (0 until subsample.size).map((itemIdx: Int) => {
        new WeightedItem[D](subsample(itemIdx).item, counts(itemIdx))
      })
    }

    def resampleSingleSubsample(splitIdx: Int)(indexSubsamplePair: (Seq[WeightedItem[D]], Int)): Seq[Seq[WeightedItem[D]]] = {
      val subsampleIdx = splitIdx + indexSubsamplePair._2
      val subsample = indexSubsamplePair._1
      val subsampleCount: Double = subsample.map(_.weight).sum
      val samplingProbabilities = subsample.map(_.weight / subsampleCount)
      //TODO: Could parallelize this.
      (0 until r).map(doSingleResample(subsampleIdx, subsample, samplingProbabilities))
    }

    def resampleSinglePartition(splitIdx: Int, partition: Iterator[Seq[WeightedItem[D]]]): Iterator[Seq[Seq[WeightedItem[D]]]] = {
      partition.zipWithIndex.map(resampleSingleSubsample(splitIdx))
    }

    val a = subsamples.collect
    subsamples.mapPartitionsWithSplit(resampleSinglePartition)
  }

  /**
   * @return an average of xi over @samples.
   * @param samples is an RDD of subsamples, with a large number of resamples for
   *   each subsample.  For example, @samples[0] will be a list of r resamples.
   *   @samples[0][0] is a resample of total weight n.  @samples[0][0][0] is a
   *   particular weighted element from a resample.
   * @param theta is the estimator function, like SUM.  It produces values of type @R.
   * @param xi is the variability function, like percentile or stddev.  It acts on a
   *   list of theta-values computed for a single subsample, producing a single
   *   value of type S.  Then the values of @xi on each subsample are averaged
   *   together using @average.
   */
  def blb[D,R,S: ClassManifest](samples: RDD[Seq[Seq[WeightedItem[D]]]],
                                theta: (Seq[WeightedItem[D]] => R),
                                xi: (Seq[R] => S),
                                average: (Seq[S] => S)): S = {
    val b = samples.collect
    val estimates: RDD[Seq[R]] = samples.map({subsample: Seq[Seq[WeightedItem[D]]] => subsample.map(theta)})
    val c = estimates.collect
    val xiValues: Seq[S] = estimates.map(xi).collect
    average(xiValues)
  }

  /**
   * Convert an ordinary aggregator into one that accepts weighted items.
   * The weights must be integral.
   *
   * The point of this is that blb() supports only aggregators that accept
   * weighted items, but some aggregators are not like that.  Aggregators
   * that are converted in this way will take O(totalWeight) time rather than
   * O(totalNumberOfItems) time.  One bright spot is that we still only need to
   * use O(totalNumberOfItems) memory for the data that are being aggregated.
   *
   * @return an aggregator that accepts weighted items.
   */
  def aggregatorToVsAggregator[D, R](aggregator: (Iterable[D] => R)): (Seq[WeightedItem[D]] => R) = {
    (weightedData: Seq[WeightedItem[D]]) => {
      aggregator(new WeightedRepeatingIterable[D](weightedData))
    }
  }
}
