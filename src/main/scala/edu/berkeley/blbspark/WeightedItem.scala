package edu.berkeley.blbspark

case class WeightedItem[D](val item: D, val weight: Double)
object WeightedItem {
  def single[D](item: D) = WeightedItem[D](item, 1.0)
}