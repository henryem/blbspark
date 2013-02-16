package edu.berkeley.blbspark.sampling

import java.util.Random
import edu.berkeley.blbspark.GroupLabeledItem

object GroupedReservoirSampling {
  //FIXME: Document
  def sample[D: ClassManifest](
      dataset: Iterator[D],
      groupCounts: Seq[Int],
      n: Int,
      rand: Random):
      Seq[GroupLabeledItem[D]] = {
    val sampleTotal = groupCounts.sum
    require(sampleTotal >= 0)
    require(sampleTotal <= n)

    //TODO: Only really need to allocate 1 array here, but right now 2 are used.
    val reservoir: Array[D] = dataset.take(sampleTotal).toArray
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
    val groupLabeledReservoir: Array[GroupLabeledItem[D]] = new Array[GroupLabeledItem[D]](sampleTotal)
    var currentReservoirIdx = 0
    for (groupIdx <- (0 until groupCounts.size)) {
      val groupCount = groupCounts(groupIdx)
      for (ignored <- (0 until groupCount)) {
        groupLabeledReservoir(currentReservoirIdx) = GroupLabeledItem(reservoir(currentReservoirIdx), groupIdx)
        currentReservoirIdx += 1
      }
    }
    groupLabeledReservoir
  }
}