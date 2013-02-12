package edu.berkeley.blbspark

import dist.MultinomialDistribution
import edu.berkeley.blbspark.util.WeightedRepeatingIterable
import scala.util.Random
import spark.RDD

object Blb {
    /**
   * @return an RDD of size @s, each element a subsample from @originalData of
   *   size @n^@alpha.
   *
   * Some notes on performance: To avoid complicated communication among slaves,
   * this function does O(@s*m) work on the driver machine, where m is the number
   * of slave machines on which the data resides.

   * @param originalData is the data to be sampled.
   * @param n is the total weight of elements of @originalData.  For example, it can
   *   be calculated with:
   *     originalData.cache()
   *     val n = originalData.map(_.weight).fold(0.0)(_ + _).asInstanceOf[Int]
   * @param alpha determines the size of each subsample: @n^@alpha
   * @param s is the number of subsamples taken.  An important constraint is that
   *   @s * @n^@alpha <= @n.  This is necessary because subsampling is done
   *   without replacement, so only @n total things can be sampled.
   */
  //TODO: @n should be allowed to be a Double instead.
  def makeBlbSubsamples[D: ClassManifest](originalData: RDD[WeightedItem[D]], n: Int, alpha: Double, s: Int, numSplits: Int): RDD[Seq[WeightedItem[D]]] = {
    val subsampleSize = math.round(math.pow(n, alpha)).asInstanceOf[Int]
    val seed = new Random().nextInt()
    val flatSamples: RDD[GroupLabeledItem[WeightedItem[D]]] = ExactSampling.sampleRepeatedlyWithWeights(
      originalData,
      false,
      Array.fill(s)(subsampleSize),
      seed)
    flatSamples
      .groupBy(_.group, numSplits)
      .map(_._2.map(_.item))
  }

  /**
   * @return a deep RDD of resamples, with r resamples for each subsample.  For
   *   example, @result[0] will be a list of r resamples corresponding to
   *   @subsamples[0]. @result[0][0] is a resample from @subsamples[0] having
   *   total weight n.  @samples[0][0][0] is a particular weighted element from
   *   the first resample of @subsamples[0].  Its weight will be an integer.
   *
   * @param subsamples is an RDD of subsamples from some original dataset.
   *   (Technically, of course, any RDD of sequences of weighted items will do,
   *   but the intent is that these are disjoint subsamples from some original
   *   dataset.)
   * @param r is the number of resamples that will be taken for each subsample.
   * @param n is the size of each resample.  n can be greater than the size of
   *   a particular subsample, in which case some elements of that subsample
   *   will be replicated (or rather, given higher weight than they already
   *   have).
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

    subsamples.mapPartitionsWithSplit(resampleSinglePartition)
  }

  /**
   * @return an average of @xi over @samples.
   * @param samples is an RDD of subsamples, with a large number of resamples for
   *   each subsample.  For example, @samples[0] will be a list of r resamples.
   *   @samples[0][0] is a resample of total weight n.  @samples[0][0][0] is a
   *   particular weighted element from a resample.
   * @param theta is the estimator function, like SUM.  It produces values of type @R.
   * @param xi is a function from a distribution on estimates of type @R to some
   *   property of interest for the distribution.  For example, this might compute a
   *   95% confidence interval or a stddev.  It acts on a list of theta-values computed
   *   for a single subsample, producing a single value of type @S.  Then the values of
   *   @xi on each subsample are averaged together using @average, producing a final
   *   estimate of the true value of @xi for this dataset and estimator.
   */
  def blb[D,R,S: ClassManifest](samples: RDD[Seq[Seq[WeightedItem[D]]]],
                                theta: (Seq[WeightedItem[D]] => R),
                                xi: (Seq[R] => S),
                                average: (Seq[S] => S)): S = {
    val estimates: RDD[Seq[R]] = samples.map({subsample: Seq[Seq[WeightedItem[D]]] => subsample.map(theta)})
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
   * @return an aggregator that accepts weighted items but otherwise computes
   *   the same thing as @aggregator.
   */
  def aggregatorToVsAggregator[D, R](aggregator: (Iterable[D] => R)): (Seq[WeightedItem[D]] => R) = {
    (weightedData: Seq[WeightedItem[D]]) => {
      aggregator(new WeightedRepeatingIterable(weightedData))
    }
  }
}
