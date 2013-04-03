package edu.berkeley.blbspark.rdd

import spark.{TaskContext, Partition, OneToOneDependency, RDD}
import edu.berkeley.blbspark.{GroupLabeledItem, WeightedItem}
import collection.Iterator
import edu.berkeley.blbspark.dist.{RandomPartitionDistribution}
import java.util.Random
import edu.berkeley.blbspark.sampling.{GroupedReservoirSampling}

class PartitionedRDDSplit(
    val originalSplit: Partition,
    val originalSplitSize: Int,
    val splitSampleSizes: Seq[Int],
    val seed: Int)
    extends Partition with Serializable {
  override def index: Int = originalSplit.index
}

//TODO: Stop ignoring weights.
class RandomPartitionedRDD[T: ClassManifest](
    val originalDataset: RDD[WeightedItem[T]],
    val sampleCounts: Seq[Int],
    val seed: Int)
    extends RDD[GroupLabeledItem[WeightedItem[T]]](originalDataset) with Serializable {

  private val cachedOriginalDataset = originalDataset.cache()
  private val originalPartitionSizes = cachedOriginalDataset
    .mapPartitions(partition => Iterator.single(partition.map(_.weight).sum.toInt))
    .collect()
  private val perPartitionSampleSizes = new RandomPartitionDistribution(originalPartitionSizes, sampleCounts, seed)
    .sample()

  override def getPartitions = {
    val rg = new Random(seed)
    (0 until originalDataset.partitions.size)
      .map(idx => {
        new PartitionedRDDSplit(originalDataset.partitions(idx), originalPartitionSizes(idx), perPartitionSampleSizes(idx), rg.nextInt)
      })
      .toArray
  }

  override def getPreferredLocations(split: Partition) =
    originalDataset.preferredLocations(split.asInstanceOf[PartitionedRDDSplit].originalSplit)

  override def compute(split: Partition, context: TaskContext) = {
    val sampleSplit: PartitionedRDDSplit = split.asInstanceOf[PartitionedRDDSplit]
    GroupedReservoirSampling.sample(originalDataset.iterator(sampleSplit.originalSplit, context), sampleSplit.splitSampleSizes, sampleSplit.originalSplitSize, new Random(seed)).iterator
  }
}