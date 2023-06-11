package DataFrameAssignment

import java.sql.Timestamp
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, datediff, dayofweek, explode, isnull, lag, lit, split, substring_index, to_date, to_timestamp, udf, unix_timestamp, weekofyear, when, year}

/**
  * Please read the comments carefully, as they describe the expected result and may contain hints in how
  * to tackle the exercises. Note that the data that is given in the examples in the comments does
  * reflect the format of the data, but not the result the graders expect (unless stated otherwise).
  */
object DFAssignment {

  /**
    * In this exercise we want to know all the commit SHA's from a list of committers. We require these to be
    * ordered according to their timestamps in the following format:
    *
    * | committer      | sha                                      | timestamp            |
    * |----------------|------------------------------------------|----------------------|
    * | Harbar-Inbound | 1d8e15a834a2157fe7af04421c42a893e8a1f23a | 2019-03-10T15:24:16Z |
    * | ...            | ...                                      | ...                  |
    *
    * Hint: Try to work out the individual stages of the exercises. This makes it easier to track bugs, and figure out
    * how Spark DataFrames and their operations work. You can also use the `printSchema()` function and `show()`
    * function to take a look at the structure and contents of the DataFrames.
    *
    * @param commits Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
    *                `println(commits.schema)`.
    * @param authors Sequence of Strings representing the authors from which we want to know their respective commit
    *                SHA's.
    * @return DataFrame of commits from the requested authors, including the commit SHA and the according timestamp.
    */
  def assignment_12(commits: DataFrame, authors: Seq[String]): DataFrame = {
    val reduced = commits.select(commits("commit.committer.name"), commits("sha"), commits("commit.author.date"))
    val renamed = reduced.withColumnRenamed("date", "timestamp").withColumnRenamed("name", "committer")
    val filtered = renamed.filter(renamed("committer").isin(authors:_*)).toDF()
    val res = filtered.orderBy(to_timestamp(col("date")))
    res
    }

  /**
    * In order to generate weekly dashboards for all projects, we need the data to be partitioned by weeks. As projects
    * can span multiple years in the data set, care must be taken to partition by not only weeks but also by years.
    *
    * Expected DataFrame example:
    *
    * | repository | week             | year | count   |
    * |------------|------------------|------|---------|
    * | Maven      | 41               | 2019 | 21      |
    * | .....      | ..               | .... | ..      |
    *
    * @param commits Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
   *                `println(commits.schema)`.
    * @return DataFrame containing 4 columns: repository name, week number, year and the number of commits for that
    *         week.
    */
  def assignment_13(commits: DataFrame): DataFrame = {
    val result = commits.select(split(col("url") , "/").getItem(5).as("repository"),
      weekofyear(col("commit.committer.date")).as("week"),
      year(col("commit.committer.date")).as("year"))

    result.groupBy(col("repository"), col("week"), col("year")).count()
  }

  /**
    * A developer is interested in the age of commits in seconds. Although this is something that can always be
    * calculated during runtime, this would require us to pass a Timestamp along with the computation. Therefore, we
    * require you to **append** the input DataFrame with an `age` column of each commit in *seconds*.
    *
    * Hint: Look into SQL functions for Spark SQL.
    *
    * Expected DataFrame (column) example:
    *
    * | age    |
    * |--------|
    * | 1231   |
    * | 20     |
    * | ...    |
    *
    * @param commits Commit DataFrame, created from the data_raw.json file.
    * @return the input DataFrame with the appended `age` column.
    */
  def assignment_14(commits: DataFrame, snapShotTimestamp: Timestamp): DataFrame = {

    val res = commits.withColumn("date", col("commit.author.date"))
      .withColumn("date", to_timestamp(col("date")))
      .withColumn("snapshot", lit(snapShotTimestamp))
      .withColumn("age", unix_timestamp(col("snapshot")) - unix_timestamp(col("date")))
      .drop("date").drop("snapshot")

    res
    }

  /**
    * To perform the analysis on commit behavior, the intermediate time of commits is needed. We require that the DataFrame
    * that is given as input is appended with an extra column. his column should express the number of days there are between
    * the current commit and the previous commit of the user, independent of the branch or repository.
    * If no commit exists before a commit, the time difference in days should be zero.
    * **Make sure to return the commits in chronological order**.
    *
    * Hint: Look into Spark SQL's Window to have more expressive power in custom aggregations.
    *
    * Expected DataFrame example:
    *
    * | $oid                     	| name   	| date                     	| time_diff 	|
    * |--------------------------	|--------	|--------------------------	|-----------	|
    * | 5ce6929e6480fd0d91d3106a 	| GitHub 	| 2019-01-27T07:09:13.000Z 	| 0         	|
    * | 5ce693156480fd0d5edbd708 	| GitHub 	| 2019-03-04T15:21:52.000Z 	| 36        	|
    * | 5ce691b06480fd0fe0972350 	| GitHub 	| 2019-03-06T13:55:25.000Z 	| 2         	|
    * | ...                      	| ...    	| ...                      	| ...       	|
    *
    * @param commits    Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
    *                   `println(commits.schema)`.
    * @param authorName Name of the author for which the result must be generated.
    * @return DataFrame with an appended column expressing the number of days since last commit, for the given user.
    */
  def assignment_15(commits: DataFrame, authorName: String): DataFrame = {
    val idNameDate = commits.select(commits("_id.$oid"), commits("commit.committer.date"))
      .withColumn("name", lit(authorName)).filter(commits("commit.committer.name") === authorName)
    val reordered = idNameDate.select(idNameDate("$oid"), idNameDate("name"), idNameDate("date"))
    val window = Window.partitionBy().orderBy("date")
    val withPrev = reordered.withColumn("prev", lag(reordered("date"), 1).over(window))
    val result = withPrev.withColumn("time_diff",
      when(isnull(datediff(to_timestamp(col("date")), to_timestamp(col("prev")))), 0)
      .otherwise(datediff(to_timestamp(col("date")), to_timestamp(col("prev")))))
    result.drop("prev")
  }

  /**
    * To get a bit of insight into the spark SQL and its aggregation functions, you will have to implement a function
    * that returns a DataFrame containing a column `day` (int) and a column `commits_per_day`, based on the commits'
    * dates. Sunday would be 1, Monday 2, etc.
    *
    * Expected DataFrame example:
    *
    * | day | commits_per_day|
    * |-----|----------------|
    * | 1   | 32             |
    * | ... | ...            |
    *
    * @param commits Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
    *                `println(commits.schema)`.
    * @return DataFrame containing a `day` column and a `commits_per_day` column representing the total number of
    *         commits that have been made on that week day.
    */
  def assignment_16(commits: DataFrame): DataFrame = {
    val result = commits.withColumn("day", dayofweek(col("commit.committer.date")))
      .groupBy(col("day")).count().select(col("day"), col("count"))
    result
  }

  /**
    * Commits can be uploaded on different days. We want to get insight into the difference in commit time of the author and
    * the committer. Append to the given DataFrame a column expressing the difference in *the number of seconds* between
    * the two events in the commit data.
    *
    * Expected DataFrame (column) example:
    *
    * | commit_time_diff |
    * |------------------|
    * | 1022             |
    * | 0                |
    * | ...              |
    *
    * @param commits Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
    *                `println(commits.schema)`.
    * @return the original DataFrame with an appended column `commit_time_diff`, containing the time difference
    *         (in number of seconds) between authorizing and committing.
    */
  def assignment_17(commits: DataFrame): DataFrame = {

    val res = commits.withColumn("authtime", to_timestamp(col("commit.author.date")))
      .withColumn("committime", to_timestamp(col("commit.committer.date")))
      .withColumn("commit_time_diff", unix_timestamp(col("committime")) - unix_timestamp(col("authtime")))
      .drop("authtime").drop("committime")
    res
    }

  /**
    * Using DataFrames, find all the commit SHA's from which a branch has been created, including the number of
    * branches that have been made. Only take the SHA's into account if they are also contained in the DataFrame.
    *
    * Note that the returned DataFrame should not contain any commit SHA's from which no new branches were made, and it should
    * not contain a SHA which is not contained in the given DataFrame.
    *
    * Expected DataFrame example:
    *
    * | sha                                      | times_parent |
    * |------------------------------------------|--------------|
    * | 3438abd8e0222f37934ba62b2130c3933b067678 | 2            |
    * | ...                                      | ...          |
    *
    * @param commits Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
    *                `println(commits.schema)`.
    * @return DataFrame containing the commit SHAs from which at least one new branch has been created, and the actual
    *         number of created branches
    */
  def assignment_18(commits: DataFrame): DataFrame = {
    val result = commits.withColumn("combined_list", explode(col("parents.sha"))).select(col("combined_list")).toDF()

    val finalResult = commits.join(result, result.col("combined_list") === commits.col("sha"))
      .groupBy(col("combined_list")).count().filter(col("count") > 1)
      .select(col("combined_list").as("sha"), col("count").as("times_parent"))
    finalResult
  }

  /**
    * In the given commit DataFrame, find all commits from which a fork has been created. We are interested in the names
    * of the (parent) repository and the subsequent (child) fork (including the name of the repository owner for both),
    * the SHA of the commit from which the fork has been created (parent_sha) as well as the SHA of the first commit that
    * occurs in the forked branch (child_sha).
    *
    * Expected DataFrame example:
    *
    * | repo_name            | child_repo_name     | parent_sha           | child_sha            |
    * |----------------------|---------------------|----------------------|----------------------|
    * | ElucidataInc/ElMaven | saifulbkhan/ElMaven | 37d38cb21ab342b17... | 6a3dbead35c10add6... |
    * | hyho942/hecoco       | Sub2n/hecoco        | ebd077a028bd2169d... | b47db8a9df414e28b... |
    * | ...                  | ...                 | ...                  | ...                  |
    *
    * Note that this example is based on _real_ data, so you can verify the functionality of your solution, which might
    * help during the debugging of your solution.
    *
    * @param commits Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
    *                `println(commits.schema)`.
    * @return DataFrame containing the parent and child repository names, the SHA of the commit from which a new fork
    *         has been created and the SHA of the first commit in the fork repository
    */
  def assignment_19(commits: DataFrame): DataFrame = {
    val col1 = udf { s: String => {
      s.split("repos/")(1)}}
    val col11 = udf { s: String => {
      s.split("/commits")(0)}}
    val col2 = commits.withColumn("child_repo_name", col11(col1(col("url")))).withColumn("parent_sha",explode(col("parents.sha")))
      .select(col("child_repo_name"),col("sha").as("child_sha"),col("parent_sha"))
    val col3 = commits.withColumn("repo_name", col11(col1(col("url"))))
      .select(col("repo_name"),col("sha"))
    val result = col2.join(col3, col2.col("parent_sha") === col3.col("sha"),"inner")
      .select("repo_name", "child_repo_name", "parent_sha" , "child_sha")
    result.filter(!(col("repo_name") === col("child_repo_name")))

  }
}
