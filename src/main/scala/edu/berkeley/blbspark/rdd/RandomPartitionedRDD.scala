package edu.berkeley.blbspark.rdd

import spark.{OneToOneDependency, RDD, Split}
import edu.berkeley.blbspark.WeightedItem
import collection.Iterator
import edu.berkeley.blbspark.dist.{GroupedReservoirSampler, RandomPartitionDistribution}
import java.util.Random
import edu.berkeley.blbspark.sampling.GroupLabeledItem

class PartitionedRDDSplit(
    val originalSplit: Split,
    val originalSplitSize: Int,
    val splitSampleSizes: Seq[Int],
    val seed: Int)
    extends Split with Serializable {
  override def index: Int = originalSplit.index
}

//TODO: Stop ignoring weights.
class RandomPartitionedRDD[T: ClassManifest](
    val originalDataset: RDD[WeightedItem[T]],
    val sampleCounts: Seq[Int],
    val seed: Int)
    extends RDD[GroupLabeledItem[WeightedItem[T]]](originalDataset.context) with Serializable {

  private val cachedOriginalDataset = originalDataset.cache()
  private val originalPartitionSizes = cachedOriginalDataset
    .mapPartitions(partition => Iterator.single(partition.map(_.weight).sum.toInt))
    .collect()
  private val perPartitionSampleSizes = new RandomPartitionDistribution(originalPartitionSizes, sampleCounts, seed)
    .sample()

  @transient
  private val splitsCache : Array[Split] = {
    val rg = new Random(seed)
    (0 until originalDataset.splits.size)
      .map(idx => {
        new PartitionedRDDSplit(originalDataset.splits(idx), originalPartitionSizes(idx), perPartitionSampleSizes(idx), rg.nextInt)
      })
      .toArray
  }
  override def splits = splitsCache

  @transient
  override val dependencies = List(new OneToOneDependency(originalDataset))

  override def preferredLocations(split: Split) =
    originalDataset.preferredLocations(split.asInstanceOf[PartitionedRDDSplit].originalSplit)

  override def compute(split: Split) = {
    val sampleSplit: PartitionedRDDSplit = split.asInstanceOf[PartitionedRDDSplit]
    //TODO: Could use (a modified form of) reservoir sampling here instead.
    // That is, currently we use a reservoir sampler to precompute the indices,
    // but we could compute the indices "on the fly" instead.  This would be
    // more efficient and simpler.
    val sampleGroups = new GroupedReservoirSampler(originalDataset.iterator(sampleSplit.originalSplit), sampleSplit.splitSampleSizes, sampleSplit.originalSplitSize, new Random(seed))
  }
}