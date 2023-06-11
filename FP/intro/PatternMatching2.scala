package intro

/**
 * PART 2 - LISTS AND PATTERN MATCHING
 * you should implement recursive functions yourself
 * using pattern matching unless an api call is explicitly allowed.
 * It is worth 14 points.
 */
object PatternMatching2 {

  /*
   * The following method shows a bit more challenging example. It is supposed
   * to represent the method to determine whether a driver will receive a fine
   * when crossing a traffic light. When the light is green, or when it is
   * orange and the time passed since crossing is less than or equal to three
   * seconds passed the driver will not receive a fine. When it is red or
   * more than three seconds after the light changed to orange, the driver
   * will receive a fine.
   *
   * The return type will indicate whether the driver gets a fine, and what
   * the reason is, as a String. This tuple, i.e. (true or false, reasoning)
   * will also indicate if the traffic light is malfunctioning. Take a look
   * below for the complete example.
   */
  def trafficLightPenalty(status: String, time : Int) : (Boolean, String) =
    (status, time) match {
      case ("green", _) 			 => (false, "ok: nothing wrong here")
      case ("orange", x) if x <= 3 => (false, "ok: we allow it")
      case ("orange", y) if y > 3  => (true, "ok: too late")
      case ("red", _) 			 => (true, "ok: waaay too late")
      case other 					 => (false, "ERROR: " + other)
    }



  /* We hope you've seen enough examples to repeat the technique on your own
   * The point to take away is that pattern matching is really if else on
   * steroids

   * For the exercises in this part you are _not_ allowed to use library
   * functions. Do not use iteration, write recursive functions instead.
   */

  /** Q5 (2p)
   * Twice takes a list and duplicates each element
   * @param xs list to map
   *
   * Example: twice(List.range(0,4)) // List(0, 0, 1, 1, 2, 2, 3, 3)
   */
  def twice[A](xs : List[A]) : List[A] = {
    xs match {
      case Nil => Nil
      case head::tail => head::head::twice(tail)
    }
  }

  /** Q6 (2p)
   * You had a few drinks too much after a party and recorded a message for
   * someone, but due to pushing a few buttons on your app now not only the
   * list of words that you speak is reversed, but the words itself are as
   * well. NB! You may use reverse function to reverse a string.
   *
   * Example:
   * 		drunkWords(List("Hey","you,","how","are","you","doing?"))
   * turns into
   * 		List("?gniod","ouy","era","woh",",ouy","yeH")
   */
  def drunkWords(xs: List[String]) : List[String] = {
    xs match {
      case Nil => Nil
      case head::tail => drunkWords(tail) ::: List(head.reverse)
    }
  }


  /** Q7 (3p)
   * MyForAll takes a list of elements, and applies a function to it, to
   * check if some condition holds. An empty list evaluates to true.
   *
   * Examples:
   * val startsWithS = (s: String) => s.startsWith("s") // lambda expression
   *
   * myForAll(List("abc", "def"), startsWithS) 					// false
   * myForAll(List("start", "strong", "system"), startsWithS) 	// true
   * NB! Note that we don't provide a test case for this exercise.
   * You are encouraged to write your own tests
   */

  ///// TO ASK ABOUT
  def myForAll[A](xs : List[A], f: A => Boolean) : Boolean = {
    xs match {
      case Nil => true
      case head::tail => if (f(head) && myForAll(tail, f)) true else false
    }

  }


  /** Q8 (3p)
   * This is the first question where you encounter the Option[T] type9
   * Use this type in the method body of lastElem, which returns an Option[A]
   * of the last element of the given List[A]
   * @param xs the list to map over
   * @return None if the list is empty or Some( .. : A), the last element of
   *   the list
   *
   * Examples:
   * 	lastElem(List()) // None
   * 	lastElem(List.range(0,3)) // Some(2) (range has exclusive ceiling)
   */
  def lastElem[A](xs : List[A]) : Option[A] = {
    xs match {
      case Nil => None
      case head::tail => if (tail != Nil) lastElem(tail)
      else Option(head)

    }
  }

  /** Q9 (4p)
   * Take two lists and concatenate them, returning the result
   * @param xs, ys, the list to concatenate
   * @return the result of first all elements from xs with all elements
   *  from ys appended
   *
   * Examples:
   * 		append(List(), List()) 			// List()
   * 		append(List(1,3,5), List(2,4)) 	// List(1,3,5,2,4)
   */
  def append[A](xs: List[A], ys: List[A]) : List[A] = {
    xs ++ ys
  }
}
