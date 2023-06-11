package intro


/**
 * PART 1 - LISTS AND PATTERN MATCHING
 * This part gives you some basic information about pattern matching.
 * For the exercises in this part you are _not_ allowed to use library functions,
 * you should implement recursive functions yourself using pattern matching.
 * It is worth 4 points.
 */
object PatternMatching1 {

  /**
   * Recursively sums all values in the list.
   *
   * @param xs the list to process.
   * @return the sum of all values in xs.
   *
   */
  def sum(xs: List[Int]): Int = {

    // Using `xs match` we "match" the value of the list.
    // This is called pattern matching.
    // For matching the whole list without any special conditions there are only two cases:
    //  Nil (or List()) and head :: tail, where head is one element and tail is a list.
    xs match {

      // The base case of the recursive definition:
      // the sum of an empty list is 0.
      case Nil => 0

      // If the list has a number,
      // add the value of the head to the sum of the tail
      case i :: tail => i + sum(tail)
    }
  }

  /**
   * N.B. Pay special attention to the following:
   * case matching can have a large number of cases; the FIRST one that
   * matches the argument mentioned before `match` is the one that will
   * `fire`. So if you have condition checks (guards, as they are called
   * in Scala) or a long list of cases to match against, beware of the order.
   *
   * Again:
   * 		!!!!! THE ORDER MATTERS IN CASE MATCHING !!!!!
   */

  /**
   * Scala also has immutable classes, called case classes. You can use
   * pattern matching on them as well.
   *
   * The following case classes are used for the next functions.
   * Note that `OptionalNum` itself cannot be instantiated (as it is abstract):
   *  it's either `None`, indicating no value, or `Num(i)`, indicating a value.
   *
   * The keyword `sealed` indicates that ONLY the two case classes below can
   * be an OptionalNum. This can be used as a safety mechanism when checking
   * pattern matching. With `sealed` forgetting a case in the pattern match
   * will result in the compiler producing a non-exhaustive matching warning
   * without the keyword, this will not happen.
   *
   * Scala has similar built-in classes:
   * `Option`, extended by `None()` and `Some(v)`.
   * This is a very useful one, since instead of `null` in Java (and the
   * Exceptions that come with it), when something is not found for example
   * you can just return None without throwing an exception.
   */
  sealed abstract class OptionalNum()

  case class Nothing() extends OptionalNum

  case class Num(i: Int) extends OptionalNum

  /**
   * Returns the sum of all defined numbers in a list of optional values.
   *
   * @param xs list of optional numeric values.
   * @return the sum of all defined numbers in `xs`.
   */
  def optionalSum(xs: List[OptionalNum]): Int = xs match {
    // Note that the `xs match` can be placed right after the function definition.

    // Case classes allow for pattern matching. Instead of an if statement to check
    // if the value is a Nothing or a Num, they can be matched in the expression like so:
    case Num(x) :: t => x + optionalSum(t)
    case Nothing() :: t => optionalSum(t)

    // Don't forget the base case!
    case Nil => 0
  }

  /** Q3 (2p)
   * Implement this function that returns the first number in xs that is divisible by n.
   * Assume n > 0.
   *
   * @param xs the list to check.
   * @param n  the positive number to divide by.
   * @return the first number in xs that is divisible by n, or Nothing if no such number exists.
   *
   *         Hint: you can use if statements in pattern matching.
   */
  def firstDivByX(xs: List[Int], n: Int): OptionalNum = {
    xs match {
      case Nil => Nothing()
      case x::tail => if (x % n == 0) Num(x)
      else firstDivByX(tail, n)
    }
  }

  /** Q4 (2p)
   * Implement this function that returns a list of only the even numbers.
   * All undefined values are left out of the output.
   *
   * @param xs the list to process.
   * @return the list of all even numbers in xs.
   */
  def onlyEvenNumbers(xs: List[OptionalNum]): List[Int] = {
    xs match {
      case Nil => Nil
      case Num(head)::tail if (head % 2 == 0) => head::onlyEvenNumbers(tail)
      case Num(head)::tail if (head % 2 != 0) => onlyEvenNumbers(tail)
      case Nothing()::tail => onlyEvenNumbers(tail)
    }
  }
}

