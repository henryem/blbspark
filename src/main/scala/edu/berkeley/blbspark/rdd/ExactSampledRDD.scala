package edu.berkeley.blbspark.rdd

import scala.collection.Iterator
import cern.jet.random.engine.DRand
import java.util.Random
import cern.jet.random.sampling.RandomSampler
import com.google.common.base.Preconditions
import spark.{OneToOneDependency, Split, RDD, TaskContext}
import edu.berkeley.blbspark.dist.{GroupedReservoirSampler, RandomPartitionDistribution, HypergeometricDistribution, MultinomialDistribution}
import edu.berkeley.blbspark.WeightedItem
import edu.berkeley.blbspark.edu.berkeley.blbspark.util.IndexFilterIterator

/**
 * @param originalSplit is what we are sampling this split from.
 * @param originalSplitSize is the size of @originalSplit.
 * @param splitSampleSize is the number of samples we want from this split
 * (currently precomputed on the master).
 */
class ExactSampledRDDSplit(
    val originalSplit: Split,
    val originalSplitSize: Int,
    val splitSampleSize: Int,
    val seed: Int)
    extends Split with Serializable {
  override def index: Int = originalSplit.index
}

/** Base class for exact-sampled RDDs.  Subclasses just implement compute(). */
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
  override def compute(split: Split): Iterator[T] = {
    val sampleSplit: ExactSampledRDDSplit = split.asInstanceOf[ExactSampledRDDSplit]
    val sampleSize = sampleSplit.splitSampleSize
    val sampleIndices: Array[Long] = Array.ofDim(sampleSize)
    val randomSource = new DRand(seed)
    // Fills in @sampleIndices with a random subset of {0, 1, ..., originalSplitSize-1}.
    //TODO: Could use reservoir sampling here instead.
    RandomSampler.sample(sampleSize, sampleSplit.originalSplitSize, sampleSize, 0, sampleIndices, 0, randomSource)
    originalDataset.iterator(split)
        .zip(new IndexFilterIterator(sampleIndices.toIterator.map(_.asInstanceOf[Int])))
        .filter(_._2)
        .map(_._1)
  }
}

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
  extends RDD[PartitionLabeledItem[WeightedItem[T]]](originalDataset.context) with Serializable {

    val cachedOriginalDataset = originalDataset.cache()
    val originalPartitionSizes = cachedOriginalDataset
        .mapPartitions(partition => Iterator.single(partition.map(_.weight).sum.toInt))
        .collect()
    val perPartitionSampleSizes = new RandomPartitionDistribution(originalPartitionSizes, sampleCounts, seed)
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
      // but we could compute the indices "on the fly" instead.
      val sampleGroups = new GroupedReservoirSampler(sampleSplit.splitSampleSizes, sampleSplit.originalSplitSize, new Random(seed))
      val sampleGroupsSeq = sampleGroups.toArray
      //FIXME: Debugging code.
      val originalSplit = originalDataset.iterator(split).toArray
      val zippedSplit = originalSplit
        .zip(sampleGroupsSeq).toArray
      val theSplit = zippedSplit
        .map(tuple => PartitionLabeledItem(tuple._1, tuple._2))
        .filter(_.partitionLabel != -1).toArray
      theSplit.toIterator
    }
}

case class PartitionLabeledItem[T](item: T, partitionLabel: Int)

