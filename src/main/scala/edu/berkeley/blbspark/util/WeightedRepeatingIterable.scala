package edu.berkeley.blbspark.util

import edu.berkeley.blbspark.WeightedItem
import com.google.common.base.Preconditions
import collection.Iterator

/**
 * An Iterable that includes a number of copies of each item in @weightedItems
 * equal to that item's weight.  For example, if @weightedItems is this:
 *   [("a", 2), ("b", 1), ("c", 0), ("a", 1)]
 * ...then the result of iterating over this collection is:
 *   ["a", "a", "b", "a"]
 *
 * @param weightedItems must have exactly integral weights.
 */
class WeightedRepeatingIterable[D](val weightedItems: Iterable[WeightedItem[D]]) extends Iterable[D] with Serializable {
  override def iterator = {
    weightedItems.iterator.flatMap((weightedItem: WeightedItem[D]) => {
      require(weightedItem.weight == math.round(weightedItem.weight))
      Iterator.empty.padTo(math.round(weightedItem.weight).asInstanceOf[Int], weightedItem.item)
    })
  }
}
