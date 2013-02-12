package edu.berkeley.blbspark.util

import com.google.common.base.Preconditions

/**
 * Grab-bag of statistics functions, to compute things like the mean of a
 * set of numerical values.
 *
 * NOTE: This is not intended for public use.
 */
object Statistics {
  def mean(data: Seq[Double]): Double = {
    val count = data.size
    Preconditions.checkArgument(count > 0)
    data.sum / count
  }

  /**
   * @param sample an IID sample from some distribution
   * @return an unbiased estimate of the variance of the distribution from
   *         which @sample was sampled
   */
  def sampleVariance(sample: Seq[Double]): Double = {
    val count = sample.size
    Preconditions.checkArgument(count > 1)
    // Special case: All elements are the same.  Numerical issues might
    // make us return something nonzero, but we should return exactly zero.
    // There's probably a better way to do this.
    if (sample.foldLeft((sample(0), true))({(agg: (Double, Boolean), value: Double) => (value, agg._2 && agg._1 == value)})._2) {
      0
    } else {
      val sum = sample.sum
      val mean = sum / count
      val sumSquares = sample.map(x => x*x).sum
      (sumSquares  - count * (mean*mean)) / (count - 1)
    }
  }

  /**
   * @param sample an IID sample from some distribution
   * @return a (slightly biased) estimate of the standard deviation of the
   *         distribution from which @sample was sampled
   */
  def sampleStdDev(sample: Seq[Double]): Double = {
    math.pow(sampleVariance(sample), 0.5)
  }
}
