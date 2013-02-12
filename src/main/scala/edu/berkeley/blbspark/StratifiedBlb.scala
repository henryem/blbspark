package edu.berkeley.blbspark

import spark.RDD
import spark.SparkContext._
import java.util.Random
import cern.jet.random.Poisson
import cern.jet.random.engine.DRand
import com.google.common.base.Preconditions

import edu.berkeley.blbspark.util.WeightedRepeatingIterable

object StratifiedBlb {

  //FIXME: Document
  //FIXME: Remove type parameters
  abstract class RddOperator[D,I,R]
  case class ArbitraryRddOperator[D,I,R](func: (RDD[D] => R)) extends RddOperator[D,I,R]
  //FIXME: Document
  case class CommutativeMonoidRddOperator[D,I,R](
      zero: I,
      projectAndAdd: ((I, D )=> I),
      add: ((I, I) => I),
      mapToResult: (I => R)
    ) extends RddOperator[D,I,R]

  /**
   * Perform the stratified bootstrap on @originalData, which is assumed
   * be a stratified sample on the strata generated by @stratifier.
   *
   * FIXME: Finish documentation.
   * FIXME: Figure out a way to make the interface less intimidating.
   *
   * @param originalData
   * @param stratifier
   * @param theta
   * @param xi
   * @param average
   * @param alpha
   * @param s
   * @param r
   * @param k
   * @param numSplits
   * @param seed
   * @tparam D
   * @tparam G
   * @tparam I
   * @tparam R
   * @tparam S
   * @return
   */
  def stratifiedBlb[D: ClassManifest, G: ClassManifest, I: ClassManifest, R, S: ClassManifest](
      originalData: RDD[WeightedItem[D]],
      stratifier: (WeightedItem[D] => G),
      theta: RddOperator[WeightedItem[D],I,R],
      xi: (Seq[R] => S),
      average: (Seq[S] => S),
      alpha: Double,
      s: Int,
      r: Int,
      k: Int,
      numSplits: Int,
      seed: Int):
      S = {
    //TODO: Figure out where to cache.
    //TODO: Warn on poor input (e.g. small k)
    //TODO: Don't need everything that splitDataByGroupSize currently computes.
    //TODO: Handle various corner cases (empty data, etc).
    //TODO: Expand API to handle finite sample correction(?)
    val dataByGroupSize = splitDataByGroupSize(originalData, stratifier, k)
    val subsampleSize: Double = math.floor(dataByGroupSize.largeGroupCounts
      .map({case (group: G, count: Int) => math.pow(count, alpha)})
      .aggregate(0.0)(_ + _, _ + _))
    val totalLargeGroupCount: Int = dataByGroupSize.largeGroupCounts
      .map({case (group: G, count: Int) => count})
      .aggregate(0)(_ + _, _ + _)
    val largeGroupSubsamples: Seq[RDD[WeightedItem[D]]] = subsample(dataByGroupSize.largeGroupData, subsampleSize, s, seed)
    val resamplingRate = totalLargeGroupCount.toFloat / subsampleSize
    val largeGroupResamples: Seq[Seq[RDD[WeightedItem[D]]]] = resample(largeGroupSubsamples, resamplingRate, r, seed)
    val thetaValues: Seq[Seq[R]] = computeEstimates(theta, largeGroupResamples, dataByGroupSize.smallGroupData)
    val xiValues: Seq[S] = thetaValues.map(xi)
    average(xiValues)
  }

  private def subsample[D: ClassManifest](data: RDD[D], subsampleSize: Double, numSubsamples: Int, seed: Int): Seq[RDD[D]] = {
    val approximatelyPartitionedData = data.mapPartitionsWithSplit(
      (splitIdx: Int, partition: Iterator[D]) => {
        val random = new Random(seed + splitIdx)
        partition.map({d => (d, random.nextInt(numSubsamples))})
      })
    (0 until numSubsamples).map({subsampleIdx => approximatelyPartitionedData.filter(_._2 == subsampleIdx).map(_._1)})
  }

  private def resample[D](subsamples: Seq[RDD[WeightedItem[D]]], samplingRate: Double, numResamples: Int, seed: Int): Seq[Seq[RDD[WeightedItem[D]]]] = {
    subsamples.map(
      (subsample: RDD[WeightedItem[D]]) => {
        (0 until numResamples).map(
          (resampleIdx: Int) => {
            subsample.mapPartitionsWithSplit(
              (splitIdx: Int, partition: Iterator[WeightedItem[D]]) => {
                val poissonDistribution = new Poisson(samplingRate, new DRand(seed + numResamples * splitIdx + resampleIdx))
                partition.map({d => WeightedItem(d.item, d.weight * poissonDistribution.nextInt())})
              })
          })
      })
  }

  private def computeEstimates[D,I: ClassManifest,R](
      theta: RddOperator[D,I,R],
      largeGroupResamples: Seq[Seq[RDD[D]]],
      smallGroupData: RDD[D]):
      Seq[Seq[R]] = {
    Preconditions.checkArgument(largeGroupResamples.size > 0)
    theta match {
      case op: ArbitraryRddOperator[D,I,R] => {
        val glommedResamples: Seq[Seq[RDD[D]]] = largeGroupResamples.map(_.map(_.union(smallGroupData)))
        glommedResamples.map(_.map(op.func))
      }
      case op: CommutativeMonoidRddOperator[D,I,R] => {
        // If the operator @theta has additional structure, we can be more
        // efficient.  We can compute the result on the small-group data once
        // and then add it to each of the results from the large-group data
        // resamples.
        val smallDataIntermediateResult = smallGroupData.aggregate(op.zero)(op.projectAndAdd, op.add)
        val largeDataIntermediateResults = largeGroupResamples.map(_.map(_.aggregate(op.zero)(op.projectAndAdd, op.add)))
        largeDataIntermediateResults.map(_.map({intermediate => op.mapToResult(op.add(smallDataIntermediateResult, intermediate))}))
      }
    }
  }

  case class DataByGroupSize[T, G](
    groupCounts: RDD[(G, Int)],
    smallGroupData: RDD[T],
    smallGroupCounts: RDD[(G, Int)],
    largeGroupData: RDD[T],
    largeGroupCounts: RDD[(G, Int)])

  private def splitDataByGroupSize[T: ClassManifest,G: ClassManifest](data: RDD[T], grouper: (T => G), k: Int): DataByGroupSize[T, G] = {
    val groupedData: RDD[(G, T)] = data.map({t: T => (grouper(t), t)})
    val dataGroupCounts: RDD[(G, Int)] = groupedData.combineByKey({t: T => 1}, {(c: Int, t: T) => c + 1}, _ + _)
    //TODO: Ensure these are properly HashPartitioned by key.
    val dataWithCounts = groupedData.join(dataGroupCounts).map(_._2)
    //TODO: Not sure whether it's better to cache dataWithCounts or not.
    // It could use too much memory.  But there definitely needs to be
    // caching here somewhere.
    val smallGroupData = dataWithCounts.filter(_._2 < k).map(_._1)
    val largeGroupData = dataWithCounts.filter(_._2 >= k).map(_._1)
    DataByGroupSize(
      dataGroupCounts,
      smallGroupData,
      dataGroupCounts.filter(_._2 < k),
      largeGroupData,
      dataGroupCounts.filter(_._2 >= k))
  }
}
