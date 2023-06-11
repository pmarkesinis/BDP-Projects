import java.text.SimpleDateFormat
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import util.Protocol.{Commit, CommitGeo, CommitSummary, File}
import util.{CommitGeoParser, CommitParser}

import java.util



/** Do NOT rename this class, otherwise autograding will fail. **/
object FlinkAssignment {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  def main(args: Array[String]): Unit = {

    /**
      * Setups the streaming environment including loading and parsing of the datasets.
      *
      * DO NOT TOUCH!
      */
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // Read and parses commit stream.
    val commitStream =
      env
        .readTextFile("data/flink_commits.json")
        .map(new CommitParser)

    // Read and parses commit geo stream.
    val commitGeoStream =
      env
        .readTextFile("data/flink_commits_geo.json")
        .map(new CommitGeoParser)

    /** Use the space below to print and test your questions. */
    question_eight(commitStream, commitGeoStream).print()

    /** Start the streaming environment. **/
    env.execute()
  }

  /** Dummy question which maps each commits to its SHA. */
  def dummy_question(input: DataStream[Commit]): DataStream[String] = {
    input.map(_.sha)
  }

  /**
    * Write a Flink application which outputs the sha of commits with at least 20 additions.
    * Output format: sha
    */
  def question_one(input: DataStream[Commit]): DataStream[String] = {
    val result = input.filter(x => x.stats.get.additions >= 20)
    result.map(_.sha)
  }

  /**
    * Write a Flink application which outputs the names of the files with more than 30 deletions.
    * Output format:  fileName
    */
  def question_two(input: DataStream[Commit]): DataStream[String] = {
    val result = input.flatMap(x => x.files)
      .filter(x => x.deletions > 30)
      .map(_.filename.getOrElse(""))

    result
    }

  /**
    * Count the occurrences of Java and Scala files. I.e. files ending with either .scala or .java.
    * Output format: (fileExtension, #occurrences)
    */
  def question_three(input: DataStream[Commit]): DataStream[(String, Int)] = {
    val result = input.flatMap(x => x.files)
    result.map(x => (x.filename.get))
      .map(x => (x.split("\\.")last, 1)).filter(x => x._1.endsWith("java") || x._1.endsWith("scala"))
      .keyBy(x => x._1).reduce((x,y) => (x._1, x._2 + y._2))
  }

  /**
    * Count the total amount of changes for each file status (e.g. modified, removed or added) for the following extensions: .js and .py.
    * Output format: (extension, status, count)
    */
  def question_four(input: DataStream[Commit]): DataStream[(String, String, Int)] = {
    val filteredFiles = input.flatMap(x => x.files)
      .filter(x => x.filename.isDefined)
      .filter(x => x.status.isDefined)
      .filter(x => x.filename.getOrElse("").split("\\.").last == "js" || x.filename.getOrElse("").split("\\.").last == "py")

    val result = filteredFiles.map(x => (x.filename.get.split("\\.").last, x.status.get, x.changes))
      .keyBy(0,1).sum(2)

    result
    }

  /**
    * For every day output the amount of commits. Include the timestamp in the following format dd-MM-yyyy; e.g. (26-06-2019, 4) meaning on the 26th of June 2019 there were 4 commits.
    * Make use of a non-keyed window.
    * Output format: (date, count)
    */
  def question_five(input: DataStream[Commit]): DataStream[(String, Int)] = {
    val sdf = new SimpleDateFormat("dd-MM-yyyy")
    val result = input.map(x => (x.commit.committer.date, 1)).assignAscendingTimestamps(_._1.getTime)
      .map(x => (x._2, sdf.format(x._1))).timeWindowAll(Time.days(1)).sum(0)

    result.map(x => (x._2, x._1))
    }

  /**
    * Consider two types of commits; small commits and large commits whereas small: 0 <= x <= 20 and large: x > 20 where x = total amount of changes.
    * Compute every 12 hours the amount of small and large commits in the last 48 hours.
    * Output format: (type, count)
    */
  def question_six(input: DataStream[Commit]): DataStream[(String, Int)] = {
    val result = input.assignAscendingTimestamps(x => x.commit.committer.date.getTime)
      .filter(x => x.stats.isDefined)
      .map(x => x.stats.get.total)
      .map(x => if(x > 20) "large" else "small")
      .map(x => (x, 1))
      .keyBy(0)
      .window(SlidingEventTimeWindows.of(Time.hours(48), Time.hours(12)))
      .sum(1)

    result
    }

  /**
    * For each repository compute a daily commit summary and output the summaries with more than 20 commits and at most 2 unique committers. The CommitSummary case class is already defined.
    *
    * The fields of this case class:
    *
    * repo: name of the repo.
    * date: use the start of the window in format "dd-MM-yyyy".
    * amountOfCommits: the number of commits on that day for that repository.
    * amountOfCommitters: the amount of unique committers contributing to the repository.
    * totalChanges: the sum of total changes in all commits.
    * topCommitter: the top committer of that day i.e. with the most commits. Note: if there are multiple top committers; create a comma separated string sorted alphabetically e.g. `georgios,jeroen,wouter`
    *
    * Hint: Write your own ProcessWindowFunction.
    * Output format: CommitSummary
    */
  def question_seven(commitStream: DataStream[Commit]): DataStream[CommitSummary] = {
    commitStream.filter(x => x.stats.isDefined).assignAscendingTimestamps(_.commit.committer.date.getTime)
      .map(x => (x.url.split("/")(4) + "/" + x.url.split("/")(5), x))
      .keyBy(_._1).timeWindow(Time.days(1)).process(new MyProcessWindowFunction()).filter(x => x.amountOfCommits > 20 && x.amountOfCommitters <= 2)
  }

  class MyProcessWindowFunction extends ProcessWindowFunction[(String, Commit), CommitSummary, String, TimeWindow] {
    override def process(key: String, context: Context, in: Iterable[(String, Commit)], result: Collector[CommitSummary]) = {
      val sdf = new SimpleDateFormat("dd-MM-yyyy")
      val date = sdf.format(context.window.getStart)
      val changes = in.filter(x => x._2.stats.isDefined).map(x => x._2.stats.get.total).sum
      val numberOfCommitters = in.map(x => x._2.commit.committer.name).toList.distinct.size
      val numberOfCommits = in.size
      val committer = in.map(x => x._2.commit.committer.name).groupBy(identity).mapValues(_.size).toList.maxBy(_._2)
      val committers = in.map(x => x._2.commit.committer.name).groupBy(identity).mapValues(_.size).toList.filter(x => x._2 == committer._2)
        .map(x => x._1).sorted.reduce((x, y) => x + "," + y)
      result.collect(CommitSummary(key, date, numberOfCommits, numberOfCommitters, changes, committers))
    }
  }

  /**
    * For this exercise there is another dataset containing CommitGeo events. A CommitGeo event stores the sha of a commit, a date and the continent it was produced in.
    * You can assume that for every commit there is a CommitGeo event arriving within a timeframe of 1 hour before and 30 minutes after the commit.
    * Get the weekly amount of changes for the java files (.java extension) per continent. If no java files are changed in a week, no output should be shown that week.
    *
    * Hint: Find the correct join to use!
    * Output format: (continent, amount)
    */
  def question_eight(
                      commitStream: DataStream[Commit],
                      geoStream: DataStream[CommitGeo]): DataStream[(String, Int)] = {

    val joined = commitStream.assignAscendingTimestamps(x => x.commit.committer.date.getTime)
      .keyBy(x => x.sha)
      .intervalJoin(geoStream.assignAscendingTimestamps(x => x.createdAt.getTime).keyBy(x => x.sha)).between(Time.hours(-1), Time.minutes(30))
      .process(new ProcessJoinFunction[Commit, CommitGeo, (String, List[File])] {
        override def processElement(left: Commit, right: CommitGeo, ctx: ProcessJoinFunction[Commit, CommitGeo, (String, List[File])]#Context, out: Collector[(String, List[File])]): Unit = {
          out.collect((right.continent, left.files))
        }})

    val res = joined.flatMap(x => x._2.map((x._1, _)))
      .filter(x => x._2.filename.isDefined)
      .filter(x => x._2.filename.get.split("\\.").last == "java")
      .map(x => (x._1, x._2.changes))
      .keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.days(7)))
      .sum(1)

    res
    }

  /**
    * Find all files that were added and removed within one day. Output as (repository, filename).
    *
    * Hint: Use the Complex Event Processing library (CEP).
    * Output format: (repository, filename)
    */
  def question_nine(
      inputStream: DataStream[Commit]): DataStream[(String, String)] = {
    val input = inputStream
      .assignAscendingTimestamps(x => x.commit.committer.date.getTime).flatMap(x => x.files.filter(x => x.status.getOrElse("").equals("added") || x.status.getOrElse("").equals("removed"))
      .map(k => ((k.filename.get, x.url.split("repos/")(1).split("/commits/")(0)), k.status.getOrElse("")))).keyBy(_._1)
    val pattern = Pattern.begin[((String, String), String)]("added").where(_._2.equals("added"))
      .next("removed").where(_._2.equals("removed")).within(Time.days(1))
    val patternStream = CEP.pattern(input, pattern)
    val result = patternStream.process(
      new PatternProcessFunction[((String, String), String), (String, String)]() {
        override def processMatch(map: util.Map[String, util.List[((String, String), String)]],
                                  ctx: PatternProcessFunction.Context,
                                  out: Collector[(String, String)]): Unit = {
          out.collect((map.get("added").get(0)._1._2, map.get("added").get(0)._1._1))
        }
      })
    result
  }

}
