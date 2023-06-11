package intro

/**
 * This part teaches you about writing functions in Scala.
 * It is worth 3 points.
 */
object Functions {

  /**
   * An example to show what function definitions look like in Scala.
   *
   * @param i a number
   * @return i * 5 + 2
   */
  def simpleMath(i: Int): Int = {

    // In functional programming variables are not used, only constants.
    // In Scala these are called `val` (for value). They cannot be reassigned.
    val temp = i * 5

    // The `return` keyword is optional:
    // the last value produced is returned automatically
    temp + 2
  }


  /** Q0 (1p)
   * Define the function `fizzBuzz` that operates as follows:
   *
   * @param i an integer number
   * @return
   *           - "Fizz" if the given number is divisible by 3
   *           - "Buzz" if the given number is divisible by 5
   *           - "FizzBuzz" if the given number is divisible by both 3 and 5
   *           - Otherwise return the string representation of the number, e.g. "2"
   */

  def fizzBuzz(i: Int): String = {
    if (i % 3 == 0 && i % 5 == 0) {
      return "FizzBuzz"
    }
    else if (i % 3 == 0) {
      return "Fizz"
    }
    else if (i % 5 == 0) {
      return "Buzz"
    }
    else {
      return i.toString;
    }
  }


  /**
   * Scala supports both `val`/`var` variables (There is actually one more type,
   * but that is outside of the scope of this course).
   * The difference is that once you assign a value to a `val` it cannot be
   * reassigned. If you would use `var` you can reassign its value though.
   * Functional Programming purists will consider var as blasphemy, but Scala
   * is a bit more pragmatic supporting both imperative programming with vars
   * and mutability as well as the functional style with `val`s and
   * immutability. Within this lab assignment you are *NOT* allowed to use
   * `var` to help speed up the functional programming learning process.
   *
   * Instead of the above method body between braces you could also write more
   * minimalistic:
   *
   * 		def simpleMath(i: Int) = i * 5 + 2
   *
   * The above expression can also be defined as an anonymous function and
   * assigned to a variable, i.e. the anonymous function (or lambda or lambda
   * expression as it is also called) can be written as follows:
   *
   * 		(i: Int) => i * 5 + 2
   *
   * This can be assigned to a variable called simpleMath by:
   *
   * 		val simpleMath = (i: Int) => i * 5 + 2
   *
   * So from this example you see that defining an anonymous function is done
   * by first supplying the input parameters between () separated by commas,
   * followed by a '=>', ending with the method body.
   *
   * You can apply such a function for example to a list by supplying it to
   * methods that take a function as input parameter. Be careful of the
   * correct function signature or else your compiler/interpreter/IDE
   * will complain.
   *
   * Example usage:
   * 		List(22,34,48).map(increaseGrade) // results in List(112, 172, 242)
   *
   * This example takes our lambda and applies it to each element of the list
   * It works because:
   * 		1. List.map takes a function as input parameter
   * 		2. final def map[B](f: (A) => B): List[B] is the function signature
   *		       of map. It takes a function (A) => B, a function which takes a
   * 		   type A element, does something to it, and returns a type B.
   *		       Our anonymous function is of type Int => Int, so this works.
   */


  /** Q1 (2p)
   * You've seen an example of anonymous functions in Scala.
   * Given the following two higher order functions (HOFs), i.e.
   * functions that take functions as input parameter, replace the ??? of the
   * anonymous functions with methods bodies and provide the correct
   * function signature for `myFirstHOF`/`mySecondHOF` in the method body of
   * `hofs` such that running `hofs()` results in the Lists given in
   * Note1
   */

  // TODO: replace the ??? with lambda expressions (anonymous functions)
  val isEven = (i: Int) => i % 2 == 0     // tells whether given Int is even or not
  val isOdd = (i: Int) => i % 2 != 0      // same but this time if it's odd
  val timesTwo = (i: Int) => i * 2   // takes an Int and doubles it

  def hofs(): List[List[Int]] =
  // TODO change this method below:
  //
  {
    // TODO:
    // - Look up the List functions used in the Scala API
    // - change the parameter type of f in both HOFS below
    //   from ...... to the correct type
    //     - then uncomment this block and remove the `???`
    def myFirstHOF(xs: List[Int], f: Int => Boolean ) : List[Int] =
      xs.filter(f)
    def mySecondHOF(xs: List[Int], f: Int => Int ) : List[Int] =
      xs.map(f)

    val first = myFirstHOF(List(1,2,3,4), isEven)
    val second = myFirstHOF(List(1,2,3,4), isOdd)
    val third = mySecondHOF(List(10,11,12,13,14,15), timesTwo)

    val finalResult = List(first, second, third)
    finalResult
    // Scala returns the final expression, but if you just assigned it to
    // a variable the return type is Unit
  }

  /* Note 1
   * running println(hofs()) results in
   * List(List(2,4), List(1,3), List(20,22,24,26,28,30))
   *
   * It is also good for you to see that you can define methods within methods
   * (if you ever need helper methods but do not want the outer world to see,
   * use this technique, i.e. it does more or less the same as defining a
   * private helper function and calling that from with the method body)
   */

  /* Note 2:
   * As you can see from inspecting the function signature of `hofs`
   * it takes no input parameters and returns a List of List[Int]
   *
   * You don't need to specify the return type, except for recursive
   * functions; Scala will infer most return types for you. It is however good
   * practice to specify the return type when defining the function signature.
   * If you read another person's code without reading the whole method body,
   * having the complete function signature including return type is a hint to
   * its inner workings. Also, when debugging, it can really help you to view
   * the function as a blackbox with input/output types, so supplying the
   * return type has good benefits.
   */

}
