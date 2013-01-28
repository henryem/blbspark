package edu.berkeley.blbspark.sampling

import java.util.Random
import com.google.common.base.Preconditions

object GroupedReservoirSampling {
  def sample[D](
      dataset: Iterator[D],
      groupCounts: Seq[Int],
      n: Int,
      rand: Random):
      Seq[D] = {
    val sampleTotal = groupCounts.sum
    Preconditions.checkArgument(sampleTotal >= 0)
    Preconditions.checkArgument(sampleTotal <= n)

    val groupLabeledReservoir: Array[GroupLabeledItem[D]] = {
      //TODO: Only really need to allocate 1 array here, but right now 3 are used.
      val reservoir = dataset.take(sampleTotal).toArray
      // A Fisher-Yates shuffle, but only the first sampleTotal rows are
      // tracked.
      for (i <- (0 until n)) {
        val j = rand.nextInt(i+1)
        if (j < sampleTotal) {
          if (i < sampleTotal) {
            val tmp = reservoir(j)
            reservoir(j) = reservoir(i)
            reservoir(i) = tmp
          } else {
            reservoir(j) = dataset.next()
          }
        }
      }

      // Note: @reservoir is now shuffled.  We just label the first groupCounts(0)
      // reservoir items as in group 0, the next groupCounts(1) items as in group
      // 1, etc.
      val unsortedGroupLabeledReservoir: Array[GroupLabeledItem[D]] = new Array[GroupLabeledItem[D]](sampleTotal)
      var currentReservoirIdx = 0
      for (groupIdx <- (0 until groupCounts.size)) {
        val groupCount = groupCounts(groupIdx)
        for (ignored <- (0 until groupCount)) {
          unsortedGroupLabeledReservoir(currentReservoirIdx) = GroupLabeledIndex(reservoir(currentReservoirIdx), groupIdx)
          currentReservoirIdx += 1
        }
      }

      unsortedGroupLabeledReservoir.sortBy(_.index)
    }
    groupLabeledReservoir
  }
}

case class GroupLabeledItem[D](item: D, group: Int)