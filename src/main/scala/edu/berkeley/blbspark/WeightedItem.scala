package edu.berkeley.blbspark

case class WeightedItem[D](val item: D, val weight: Double)
object WeightedItem {
  /** Convenience function to produce an item with weight 1 from a single item. */
  def single[D](item: D) = WeightedItem[D](item, 1.0)
}