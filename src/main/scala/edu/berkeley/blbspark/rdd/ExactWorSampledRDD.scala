package edu.berkeley.blbspark.rdd

import spark.{TaskContext, Partition, RDD}
import collection.Iterator
import cern.jet.random.engine.DRand
import cern.jet.random.sampling.RandomSampler
import edu.berkeley.blbspark.util.IndexFilterIterator

/**
 * An RDD sampled without replacement from @originalDataset.
 *
 * @param sampleCount elements are sampled.
 */
class ExactWorSampledRDD[T: ClassManifest](
  originalDataset: RDD[T],
  sampleCount: Int,
  seed: Int)
  extends ExactSampledRDD[T](originalDataset, sampleCount, seed) with Serializable {
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val sampleSplit: ExactSampledRDDSplit = split.asInstanceOf[ExactSampledRDDSplit]
    val sampleSize = sampleSplit.splitSampleSize
    val sampleIndices: Array[Long] = Array.ofDim(sampleSize)
    val randomSource = new DRand(seed)
    // Fills in @sampleIndices with a random subset of {0, 1, ..., originalSplitSize-1}.
    //TODO: Could use reservoir sampling here instead.
    RandomSampler.sample(sampleSize, sampleSplit.originalSplitSize, sampleSize, 0, sampleIndices, 0, randomSource)
    originalDataset.iterator(split, context)
      .zip(new IndexFilterIterator(sampleIndices.toIterator.map(_.asInstanceOf[Int])))
      .filter(_._2)
      .map(_._1)
  }
}