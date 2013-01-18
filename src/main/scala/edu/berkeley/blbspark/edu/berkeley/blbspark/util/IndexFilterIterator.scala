package edu.berkeley.blbspark.edu.berkeley.blbspark.util

import scala.Iterator

/**
 * An iterator over the presence of indices in @presentIndices.  The ith call
 * to next() returns true if i is in @presentIndices, false otherwise.
 */
class IndexFilterIterator(val presentIndices: Iterator[Int]) extends Iterator[Boolean] {
  var nextPotentialIndex = 0
  var nextPresentIndex = if (presentIndices.hasNext) {
    presentIndices.next
  } else {
    -1
  }

  override def hasNext() = true
  override def next() = {
    val currentIndex = nextPotentialIndex
    nextPotentialIndex += 1
    if (nextPresentIndex == currentIndex) {
      if (presentIndices.hasNext) {
        nextPresentIndex = presentIndices.next
      }
      true
    }
    false
  }
}