import scala.annotation.tailrec

class GroupingIterator[K](internal: Iterator[(K, Int)]) extends Iterator[(K, Int)] {
  var firstInGroupOpt : Option[(K, Int)] = None

  override def hasNext: Boolean = firstInGroupOpt.nonEmpty || internal.hasNext

  override def next(): (K, Int) = {
    val firstInGroup = firstInGroupOpt.getOrElse(internal.next())

    @tailrec
    def loop(accum: Int, current: (K, Int)): (Int, (K, Int)) = {
      if (internal.hasNext && current._1 == firstInGroup._1) {
        loop(accum + current._2, internal.next())
      } else (accum, current)
    }

    val (accum, current) = loop(0, firstInGroup)

    val (firstOpt, acc) = if (current._1 == firstInGroup._1) (None, accum + current._2) else (Some(current), accum)
    firstInGroupOpt = firstOpt
    (firstInGroup._1, acc)
  }
}
