package fp_functions


/**
 * This part is about implementing several functions that are very common in functional programming.
 * For the exercises in this part you are _not_ allowed to use library functions.
 * Do not use iteration, write recursive functions instead.
 * Hint: read about the functions in the lecture slides.
 *
 * This part is worth 30 points, 5 points per function.
 *
 * * In summary, what you've seen so far is:
 * - pattern matching on Lists and case classes
 * - the use of API methods
 * - simple functions
 * - daisy chaining method calls just like Unix pipelines
 *
 * Next up is a few more examples I'd like to show you before you start
 * answering the more advanced questions
 *
 * 1. SUM
 *
 * def sum(xs: List[Int]) : Int = xs match {
 * case List() => 0
 * case h :: t => h + sum(t)
 * }
 *
 * Here are a few other ways of writing it:
 *
 * def sum2(xs: List[Int]) : Int = if (xs.isEmpty) 0 else xs.head + sum2(xs.tail)
 * def sumAPI1(xs: List[Int]) = xs.foldLeft(0)(_ + _)
 * def sumAPI2(xs: List[Int]) = xs.foldRight(0)(_ + _)
 * def sumAPI3(xs: List[Int]) = xs.sum
 *
 * def sumIterative(xs: List[Int]) : Int = {
 * var sum  = 0;
 * for (x <- xs) {
 * sum = sum + x
 * }
 * sum
 * }
 * STOP!!! STOP!!! STOP!!! Remember no var?, the for expression is fine, it's
 * just that this method mutates the `sum` var and we don't allow that.
 *
 * 2. Folds
 *
 * the foldLeft/foldRight are an interesting bunch. They can be very powerful
 * what the foldLeft does in this example is take from left to right, two
 * elements from the list and feed it as parameters into the function.
 * In this case `_ + _` is the shorthand for ( (x: Int, y:Int) => x + y)
 * Some people call these reduce, but that is not correct. While similar
 * please see the following outputs (I've left out the `println`):
 *
 * // foldLeft
 * List.range(0,5).foldLeft(0)(_ + _) 					// 10 with a shorthand lambda
 * List.range(0,5).foldLeft(0)((x:Int, y:Int) => x+y) 	// 10
 * List(1).foldLeft(0)((x:Int, y:Int) => x+y) 			// 1
 * List().foldLeft(0)((x:Int, y:Int) => x+y) 			// 0
 *
 * // compiler will warn about overloaded method and compiler doesn't know
 * // what to pick, so use the fully written lambda to prevent such warnings
 * List().foldLeft(0)(_ + _))
 *
 * // reduceLeft
 * // type is needed or else warning, but with type still an exception
 * List[Int]().reduceLeft(_+_)
 *
 * List(7).reduceLeft(_+_) 								// 7
 * List(1,4,5).reduceLeft(_+_) 							// 10
 *
 * As you can see there are some minor, but tricky differences
 * Another one to note is if your function isn't associative, then
 * foldLeft (going from left to right), will give you another result than
 * foldRight (going from right to left). You're highly recommended to look up
 * some more examples.
 *
 * 3. Flattening/Options
 *
 * Next I'd like to show you a few things that can happen when working with
 * Options, some of which aren't quite straightforward at first.
 *
 * Say you have a variable called results of type List[Option[Int]] and want
 * an easy way to filter out the Nones:
 *
 * val results = List(Some(6), Some(8), None, None, Some(9))
 *
 * results.flatten 			 // List(6,8,9) the Options are 'unwrapped', i.e. Some peeled off and None thrown out
 * results.flatMap(x => x) 	 // does the same (first calls map, then flattens the result)
 * results.flatMap(identity) // syntactic sugar for `x => x`
 *
 * while this is very cool to see Option unwrapped, below are a few more examples
 * which show that working with Option sometimes can be a bit challenging.
 *
 * Let's say you have a competition and with some different rounds.
 * The administration kept track of contestants name age and hobby as a
 * Tuple3(String, Int, String). The finale is aired on tv and the host
 * only cares about the name and age of the contestants. He has a list of
 * the persons who qualify for the final. If a contestant did not make it through
 * the last round, `None` is registered. The final contestants look like this:
 *
 * val contestants = List(
 * Some(("Richard", 26, "windsurfing")),
 * None,
 * Some(("Amy", 22, "bmx"))
 * )
 *
 * If the tv show host only wants their name and age:
 *
 * contestants.flatMap(x => if (x.isDefined) Some(x.get._1, x.get._2) else None)// List((Richard,26), (Amy,22))
 *
 * other things the host could do:
 *
 * val richard = contestants(0).get 											// (Richard, 26, windsurfing)
 * //val oops = contestants(1).get 												// exception, because of None.get
 * val contestant = if (contestants(1).isDefined) contestants(1).get else None 	// None
 *
 * BEWARE OF TRANSFORMATIONS!!
 *
 * // if contestants failed the earlier rounds, they get transformed from `None`
 * // to "failedContestant"
 *
 * println(contestants.flatMap(y => y match {
 * case None => "failedContestant"
 * case Some((name, age, hobby)) => Some(name, age)
 * }))
 * // results in List((Richard,26), f, a, i, l, e, d, C, o, n, t, e, s, t, a, n, t, (Amy,22))
 *
 * Forgetting Some in case Some results in a compiler warning; Scala can't
 * flatMap the (name, age), but the trickier issue is the String in the current
 * implementation. Strings can be flatmapped resulting in the characters
 * printed out separately. Beware of this and try to build your solution
 * step by step!
 *
 * 4. Case Classes
 *
 * There is one last thing I'd like to show you before getting to the questions
 * Simple definition of a case class and one instance:
 *
 * case class Book(isbn13 : String, translations: List[String], title: String)
 * val lotr = Book("9780395647394", List("EN", "NL", "FR"), "Lord of the Rings, Part 2: The Two Towers")
 *
 * if you want the list of languages into which it was translated call:
 * val translatedTo = lotr.translations
 *
 * i.e. you can use the .fieldName of the case class to get that specific field!
 *
 * You can read more about case classes here:
 * - https://docs.scala-lang.org/tour/case-classes.html
 * - https://docs.scala-lang.org/overviews/scala-book/case-classes.html
 *
 * 5. Tuples and indexing
 *
 * Last thing for me to tell you is an example with tuples. Scala supports up
 * to Tuple22 and you can index each element of a tuple with ._indexnr
 *
 * val ingredient = ("Sugar", 25)
 * println(ingredient._2) // outputs 25
 *
 * You can also nest these, for example:
 *
 * val recipe = (("tangerine", 2.0),("celery", 0.25),("cucumber", 1.0))
 *
 * println(recipe._1) 		// (tangerine,2.0)
 * println(recipe._2._2) 	// 0.25
 *
 * so far so good, but...    are you ready? ... the type of recipe is...
 *
 * Tuple3[Tuple2[String, Double], Tuple2[String, Double],Tuple2[String, Double]]
 *
 * yes, that is mindboggling for something so simple...
 * So if you ever need to work with tuples, sometimes it's good to just take
 * one element, write it out or sort it out and see how to get to the element
 * you are interested in. Scastie (online Scala interpreter) is a good scratchpad
 * to try your scribbling out on...
 *
 * That's about the end of me ranting and trying to show you some nice Scala
 * tips and tricks. Good luck!
 */
object FPFunctions {

    /** Q14 (5p)
     * Applies `f` to all elements and returns a new list.
     * @param xs list to map.
     * @param f mapping function.
     * @tparam A type of items in xs.
     * @tparam B result type of mapping function.
     * @return a list of all items in `xs` mapped with `f`.
     */
    def map[A, B](xs: List[A], f: A => B): List[B] = {
        xs match {
            case Nil => Nil
            case head::tail => f(head)::map(tail, f)
        }
    }

    /** Q15 (5p)
     * Takes a function that returns a boolean and returns all elements that satisfy it.
     * @param xs the list to filter.
     * @param f the filter function.
     * @tparam A the type of the items in `xs`.
     * @return a list of all items in `xs` that satisfy `f`.
     */
    def filter[A](xs: List[A], f: A => Boolean): List[A] = {
        xs match {
            case Nil => Nil
            case head::tail if (f(head)) => head::filter(tail, f)
            case head::tail if (!f(head)) => filter(tail, f)
        }
    }

    /** Q16 (5p)
     * Recursively flattens a list that may contain more lists into 1 list.
     * Example:
     *     List(List(1), List(2, 3), List(4)) -> List(1, 2, 3, 4)
     * @param xs the list to flatten.
     * @return one list containing all items in `xs`.
     */
    def recFlat(xs: List[Any]): List[Any] = {

        xs match {
            case Nil => Nil
            case (head:Int)::tail => head :: recFlat(tail)
            case (head:List[_])::tail => recFlat(head) ++ recFlat(tail)
        }
    }

    /** Q17 (5p)
     * Takes `f` of 2 arguments and an `init` value and combines the elements by applying `f` on the result of each previous application.
     * @param xs the list to fold.
     * @param f the fold function.
     * @param init the initial value.
     * @tparam A the type of the items in `xs`.
     * @tparam B the result type of the fold function.
     * @return the result of folding `xs` with `f`.
     */
    def foldL[A, B](xs: List[A], f: (B, A) => B, init: B): B = {
        xs match {
            case Nil => init
            case head::tail => foldL(tail, f, f(init, head))
        }
    }

    /** Q18 (5p)
     * Reuse `foldL` to define `foldR`.
     * If you do not reuse `foldL`, points will be subtracted.
     *
     * @param xs the list to fold.
     * @param f the fold function.
     * @param init the initial value.
     * @tparam A the type of the items in `xs`.
     * @tparam B the result type of the fold function.
     * @return the result of folding `xs` with `f`.
     */
    def foldR[A, B](xs: List[A], f: (A, B) => B, init: B): B = {

        def reverse(xs: List[A]): List[A] = {
            xs match {
                case Nil => Nil
                case head::tail => reverse(tail):::List(head)

            }
        }

        def helper2(b: B, a: A): B = {
            f(a, b)
        }


        foldL(reverse(xs), helper2, init)

    }

    /** Q19 (5p)
     * Returns an iterable collection formed by iterating over the corresponding items of `xs` and `ys`.
     * If one list is shorter, stop when there are no more elements to process in this list.
     *
     * @param xs the first list.
     * @param ys the second list.
     * @tparam A the type of the items in `xs`.
     * @tparam B the type of the items in `ys`.
     * @return a list of tuples of items in `xs` and `ys`.
     */
    def zip[A, B](xs: List[A], ys: List[B]): List[(A, B)] = {
        xs match {
            case Nil => Nil
            case headX::tailX => ys match {
                case Nil => Nil
                case headY::tailY => (headX, headY)::zip(tailX, tailY)

            }
        }
    }


}
