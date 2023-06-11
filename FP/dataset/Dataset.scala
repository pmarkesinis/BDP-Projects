package dataset

import dataset.util.Commit.Commit

import java.text.SimpleDateFormat
import java.util.SimpleTimeZone
import scala.math.Ordering.Implicits._

/**
 * Use your knowledge of functional programming to complete the following functions.
 * You are recommended to use library functions when possible.
 *
 * The data is provided as a list of `Commit`s. This case class can be found in util/Commit.scala.
 * When asked for dates, use the `commit.commit.committer.date` field.
 *
 * This part is worth 40 points.
 */
object Dataset {


  /** Q23 (4p)
   * For the commits that are accompanied with stats data, compute the average of their additions.
   * You can assume a positive amount of usable commits is present in the data.
   *
   * @param input the list of commits to process.
   * @return the average amount of additions in the commits that have stats data.
   */
  def avgAdditions(input: List[Commit]): Int = {
    val commitList = input.flatMap(x => x.stats.filter(x => x != None))
    val addition = commitList.map(x => x.additions)
    val result = addition.sum/ addition.size
    result
    //    def helper(input: List[Commit], n: Int, sum: Int): Int = {
    //      input match {
    //        case Nil => sum / n
    //        case head::tail if (head.stats != None) => helper(tail, n+1, sum + helper2(head.stats.get))
    //        case head::tail if (head.stats == None) => helper(tail, n, sum)
    //      }
    //    }
    //    def helper2(stat: Stats): Int = {
    //      stat.additions
    //    }
    //
    //    helper(input, 0, 0)
  }

  /** Q24 (4p)
   * Find the hour of day (in 24h notation, UTC time) during which the most javascript (.js) files are changed in commits.
   * The hour 00:00-00:59 is hour 0, 14:00-14:59 is hour 14, etc.
   * NB!filename of a file is always defined.
   * Hint: for the time, use `SimpleDateFormat` and `SimpleTimeZone`.
   *
   * @param input list of commits to process.
   * @return the hour and the amount of files changed during this hour.
   */
  def jsTime(input: List[Commit]): (Int, Int) = {
    val hour = new SimpleDateFormat("HH")
    hour.setTimeZone(new SimpleTimeZone(SimpleTimeZone.UTC_TIME, "UTC"))
    val hours = input.groupBy(x => hour.format(x.commit.committer.date).toInt)
    val jsHour = hours.maxBy(x => x._2.flatMap(x => x.files).count(x => x.filename.get.endsWith(".js")))
    val fileChanges = jsHour._2.flatMap(x => x.files).count(x => x.filename.get.endsWith(".js"))
    val result = (jsHour._1, fileChanges)
    result
  }


  /** Q25 (5p)
   * For a given repository, output the name and amount of commits for the person
   * with the most commits to this repository.
   * For the name, use `commit.commit.author.name`.
   *
   * @param input the list of commits to process.
   * @param repo  the repository name to consider.
   * @return the name and amount of commits for the top committer.
   */
  def topCommitter(input: List[Commit], repo: String): (String, Int) = {
    val groupNameList = input.groupBy(x => x.commit.author.name)
    val max = groupNameList.maxBy(x => x._2.count(x => x.url.contains(repo)))

    (max._1, max._2.count(x => x.url.contains(repo)))
  }

  /** Q26 (9p)
   * For each repository, output the name and the amount of commits that were made to this repository in 2019 only.
   * Leave out all repositories that had no activity this year.
   *
   * @param input the list of commits to process.
   * @return a map that maps the repo name to the amount of commits.
   *
   *         Example output:
   *         Map("KosDP1987/students" -> 1, "giahh263/HQWord" -> 2)
   */
  def commitsPerRepo(input: List[Commit]): Map[String, Int] = {
    val with2019 = input.filter(x => x.commit.committer.date.toString.endsWith("2019"))
    val groupByRepoURL = with2019.groupBy(x => x.url.split("/repos/").last.split("/commits").head)
    //val without2022 = groupByRepoURL.filterNot(x => x._2.filter(x => !(x.commit.committer.date.toString.endsWith("2022"))) == true)
    val result = groupByRepoURL.map(x => (x._1, x._2.size))
    result

  }


  /** Q27 (9p)
   * Derive the 5 file types that appear most frequent in the commit logs.
   * NB!filename of a file is always defined.
   * @param input the list of commits to process.
   * @return 5 tuples containing the file extension and frequency of the most frequently appeared file types, ordered descendingly.
   */
  def topFileFormats(input: List[Commit]): List[(String, Int)] = {
    val fileTypes = input.flatMap(x => x.files.map(x => x.filename.get.split("\\.").last))
    val topFiles = fileTypes.groupBy(x => x).map(c => (c._1, c._2.size)).toList.sortWith(_._2 > _._2).take(5);
    topFiles
  }


  /** Q28 (9p)
   *
   * A day has different parts:
   * morning 5 am to 12 pm (noon)
   * afternoon 12 pm to 5 pm.
   * evening 5 pm to 9 pm.
   * night 9 pm to 4 am.
   *
   * Which part of the day was the most productive in terms of commits ?
   * Return a tuple with the part of the day and the number of commits
   *
   * Hint: for the time, use `SimpleDateFormat` and `SimpleTimeZone`.
   */
  def mostProductivePart(input: List[Commit]): (String, Int) = {
    def helper(commit: Commit): String = {
      val hour = new SimpleDateFormat("HH")
      hour.setTimeZone(new SimpleTimeZone(SimpleTimeZone.UTC_TIME, "UTC"))
      val partOfDay = hour.format(commit.commit.committer.date).toInt
      if (partOfDay == 5 || partOfDay == 6 ||partOfDay == 7 ||partOfDay == 8 ||partOfDay == 9 ||partOfDay == 10 ||partOfDay == 11) "morning"
      else if (partOfDay == 12 ||partOfDay == 13 ||partOfDay == 14 ||partOfDay == 15 || partOfDay == 16) "afternoon"
      else if (partOfDay == 17 || partOfDay == 18 ||partOfDay == 19 ||partOfDay == 20) "evening"
      else "noon"
    }

    val partOfDays = input.groupBy(x => helper(x))
    val result = partOfDays.maxBy(x => x._2.size)
    (result._1, result._2.size)
    //
  }
  /*{
    val hour = new SimpleDateFormat("HH")
    hour.setTimeZone(new SimpleTimeZone(SimpleTimeZone.UTC_TIME, "UTC"))
    val hours = input.groupBy(x => hour.format(x.commit.committer.date).toInt)
    val morning = hours.groupBy(x=> x._1 == 5 || x._1 == 6 || x._1 == 7 || x._1 == 8 || x._1 == 9 || x._1 == 10 || x._1 == 11)
    val afternoon = hours.groupBy(x=> x._1 == 12 || x._1 == 13 || x._1 == 14 || x._1 == 15 || x._1 == 16)
    val evening = hours.groupBy(x=> x._1 == 17 || x._1 == 18 || x._1 == 19 || x._1 == 20)
    val noon = hours.groupBy(x=> x._1 == 21 || x._1 == 22 || x._1 == 23 || x._1 == 24 || x._1 == 1 || x._1 == 2 || x._1 == 3 || x._1 == 4)
    val combine = morning.toList ++ afternoon.toList ++ evening.toList ++ noon.toList
    val addThem = combine.filter()*/

//


//    if (topHour._1 == 5 || topHour._1 == 6 || topHour._1 == 7 ||  topHour._1 == 8 ||topHour._1 == 9 ||topHour._1 == 10 ||topHour._1 == 11) {
//      ("morning", topHour._)
//    } else if (topHour._1 == 12 || topHour._1 == 13 ||topHour._1 == 14 ||topHour._1 == 15 ||topHour._1 == 16) {
//      ("afternoon", topHour._2.size)
//    } else if (topHour._1 == 17 ||topHour._1 == 18 ||topHour._1 == 19 ||topHour._1 == 20) {
//      ("evening", topHour._2.size )
//    } else ("noon", topHour._2.size)
// }
}
