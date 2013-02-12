package edu.berkeley.blbspark

import rdd.{RandomPartitionedRDD, ExactWorSampledRDD}
import spark.RDD

/** Factory methods for exact sampled RDDs. */
object ExactSampling {
  def sample[T: ClassManifest](
      originalDataset: RDD[T],
      withReplacement: Boolean,
      sampleSize: Int,
      seed: Int):
      RDD[T] = {
    if (withReplacement) {
      throw new Exception("Not implemented yet!") //FIXME
      //new ExactReplacementSampledRDD(originalDataset, sampleSize, seed)
    } else {
      new ExactWorSampledRDD(originalDataset, sampleSize, seed)
    }
  }

  /**
   * As sample(), but items have a weighted representation.  A
   * WeightedItem(X, 2.0) is treated like two instances of
   * WeightedItem(X, 1.0).  Weights must be integral.
   *
   * This is intended to perform sampling more efficiently in cases where there
   * are many copies of each datum.
   */
  def sampleWithWeights[T](
      originalDataset: RDD[WeightedItem[T]],
      withReplacement: Boolean,
      sampleSize: Int,
      seed: Int):
      RDD[WeightedItem[T]] = {
    throw new Exception("Not implemented yet!") //FIXME
  }

  /**
   * Sample repeatedly from @originalDataset.  @sampleSizes.size samples are
   * taken, the ith of size @sampleSizes(i).  Rather than partitioning the
   * samples, this method simply returns an RDD of tuples:
   *   (assignedSample, datum)
   * The user is then free to group the tuples into samples, for example via
   * a simple groupByKey().  Note that if sampling is done with replacement, a
   * datum may appear multiple times under different assigned samples or even
   * under the same assigned sample.
   *
   * If sampling is done without replacement, the repeated samples are
   * themselves nonoverlapping.  In that case, it is an error if
   * sampleSizes.sum exceeds originalDataset.count.  Currently there is no
   * support for allowing the samples to overlap but to be taken individually
   * without replacement.  For example, say that @originalDataset is
   * [4, 5, 6, 7] and @sampleSizes = [2, 2].  Then if @withReplacement is true,
   * the following are possible samples:
   *   1. [(0, 6), (0, 6), (1, 5), (1, 6)]  // The samples contain repeated items and overlap.
   *   2. [(0, 5), (0, 6), (1, 4), (1, 5)]  // The samples overlap.
   *   3. [(0, 5), (0, 6), (1, 4), (1, 7)]  // The samples contain no repeats and are disjoint.
   * If @withReplacement is false, neither #1 nor #2 are possible samples.
   */
  def sampleRepeatedly[T](
      originalDataset: RDD[T],
      withReplacement: Boolean,
      sampleSizes: Seq[Int],
      seed: Int):
      RDD[(Int, T)] = {
    if (withReplacement) {
      throw new Exception("Not implemented yet!") //FIXME
      //new RepeatedReplacementSampledRDD(originalDataset, sampleSizes, seed)
    } else {
      throw new Exception("Not implemented yet!") //FIXME
      //new RepeatedWorSampledRDD(originalDataset, sampleSizes, seed)
    }
  }

  /** As sampleRepeatedly(), but items have a weighted representation. */
  def sampleRepeatedlyWithWeights[T: ClassManifest](
                                      originalDataset: RDD[WeightedItem[T]],
                                      withReplacement: Boolean,
                                      sampleSizes: Seq[Int],
                                      seed: Int):
      RDD[GroupLabeledItem[WeightedItem[T]]] = {
    if (withReplacement) {
      throw new Exception("Not implemented yet!") //FIXME
    } else {
      new RandomPartitionedRDD[T](originalDataset, sampleSizes, seed)
    }
  }
}