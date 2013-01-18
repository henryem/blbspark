package edu.berkeley.blbspark.rdd

import scala.collection.Iterator
import java.util.Random
import spark.{OneToOneDependency, Split, RDD}
import edu.berkeley.blbspark.dist.HypergeometricDistribution

/**
 * @param originalSplit is what we are sampling this split from.
 * @param originalSplitSize is the size of @originalSplit.
 * @param splitSampleSize is the number of samples we want from this split
 *   (currently precomputed on the master).
 */
class ExactSampledRDDSplit(
    val originalSplit: Split,
    val originalSplitSize: Int,
    val splitSampleSize: Int,
    val seed: Int)
    extends Split with Serializable {
  override def index: Int = originalSplit.index
}

/**
 * Base class for exact-sampled RDDs.  Subclasses just implement compute().  Note that this is just a helper class
 * for RDDs that use exact sampling - it's not intended for direct use by end-users.
 */
abstract class ExactSampledRDD[T: ClassManifest](
    val originalDataset: RDD[T],
    val sampleCount: Int,
    val seed: Int)
    extends RDD[T](originalDataset.context) with Serializable {

  val cachedOriginalDataset: RDD[T] = originalDataset.cache()
  val originalPartitionSizes: Seq[Int] = cachedOriginalDataset
    .mapPartitions(partition => Iterator.single(partition.size))
    .collect
    .toSeq
  val perPartitionSampleSizes = new HypergeometricDistribution(originalPartitionSizes, sampleCount, seed).sample()

  override def splits = {
    val rg = new Random(seed)
    (0 until originalDataset.splits.size)
      .map(idx => {
        new ExactSampledRDDSplit(originalDataset.splits(idx), originalPartitionSizes(idx), perPartitionSampleSizes(idx), rg.nextInt)
      })
      .toArray
  }

  @transient
  override val dependencies = List(new OneToOneDependency(originalDataset))

  override def preferredLocations(split: Split) =
    originalDataset.preferredLocations(split.asInstanceOf[ExactSampledRDDSplit].originalSplit)
}
