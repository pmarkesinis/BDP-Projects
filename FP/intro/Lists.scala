package intro

/**
 * This part gives a brief introduction to how lists work in Scala.
 * It is worth 3 points.
 */
object Lists {

    // Run this main function to see the result of the `println` calls.
    def main(args: Array[String]): Unit = {
        // In Scala, lists are defined recursively. A list of the item 1 looks like this:
        val one = 1 :: Nil

        /**
         * The `::` operator concatenates an item and a list.
         * As the right side always needs to be a list, the terminating value `Nil` is added.
         * You could read the following list as `1 :: (2 :: (3 :: (Nil)))`,
         * every part in brackets is a list.
         */
        val three = 1 :: 2 :: 3 :: Nil

        /**
         * For 1 chain in the list `x :: y`, `x` is usually referred to as the head of the list, `y` is called the tail.
         * They can be accessed as follows:
         */
        val h = three.head
        val t = three.tail
        println(s"Head of $three is $h")
        println(s"Tail of $three is $t")

        /**
         * Since the lists are defined recursively, accessing a certain index is not O(1) but O(n).
         * Traversing the full list however is still O(n) and this is what is used most often in functional programming.
         *
         * Scala's lists are immutable. Any operation that should change a value will return a new list.
         * For example, increasing the value of every item with 1 looks like this:
         */

        val plusOne = three.map(x => x + 1)
        println(three) // The original list is not changed
        println(plusOne) // This is a new list


        /**
         * Lists can also be created with the `List` function. This is a shorthand for the recursive way given earlier.
         * When printing lists to the console, they are displayed like this as well.
         */
        val four = List(1, 2, 3, 4)
    }

    /** Q2 (3p)
     * Implement this function that takes the average of the first `n` items that are larger than `x`.
     * Assume `n` is a positive integer > 0.
     * Note that `n` is an upper bound, you might not have `n` usable elements.
     *
     * @param xs the list of items.
     * @param x  only use numbers strictly larger than x.
     * @param n  how many items to consider for the average.
     * @return the average of the first `n` items larger than `x`.
     *
     * Hint:
     * Read the Scaladoc on the List class (https://www.scala-lang.org/api/2.12.3/scala/collection/immutable/List.html)
     * It contains some useful functions for this exercise. For this question library functions are allowed.
     */
    def customAverage(xs: List[Int], x: Int, n: Int): Int = {
        val larger = xs.filter(_ > x)
        val firstnlist = larger.take(n)
        val sumoflist = firstnlist.sum
        val average = sumoflist/firstnlist.size

        average
    }
}
