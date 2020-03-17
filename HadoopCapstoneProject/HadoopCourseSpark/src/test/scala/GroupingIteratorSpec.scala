import org.scalatest._
import org.scalatest.matchers.should.Matchers

class GroupingIteratorSpec extends WordSpec with Matchers {
  "Grouping iterator" should {
    "group by sequential keys and sum values" in {
      val input = List(
        ("a", 1), ("a", 2), ("a", 3),
        ("b", 2), ("b", 3),
        ("c", 10))
      new GroupingIterator(input.iterator).toList shouldBe List(("a", 6), ("b", 5), ("c", 10))
    }

    "remote groups should not be summed" in {
      val input = List(
        ("a", 1), ("a", 2), ("a", 3),
        ("b", 2), ("b", 3),
        ("c", 10),
        ("a", 1), ("a", 2))
      new GroupingIterator(input.iterator).toList shouldBe List(("a", 6), ("b", 5), ("c", 10), ("a", 3))
    }

    "sum a single group" in {
      val input = List(
        ("a", 1), ("a", 2), ("a", 3))
      new GroupingIterator(input.iterator).toList shouldBe List(("a", 6))
    }

    "handle empty input" in {
      new GroupingIterator(Nil.iterator).toList shouldBe Nil
    }
  }
}