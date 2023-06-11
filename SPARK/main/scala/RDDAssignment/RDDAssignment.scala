package RDDAssignment

import java.util.UUID
import java.math.BigInteger
import java.security.MessageDigest
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import utils.{Commit, File, Stats}

object RDDAssignment {


  /**
    * Reductions are often used in data processing in order to gather more useful data out of raw data. In this case
    * we want to know how many commits a given RDD contains.
    *
    * @param commits RDD containing commit data.
    * @return Long indicating the number of commits in the given RDD.
    */
  def assignment_1(commits: RDD[Commit]): Long = {
    val l: Long = commits.count;
    l
  }

  /**
    * We want to know how often programming languages are used in committed files. We want you to return an RDD containing Tuples
    * of the used file extension, combined with the number of occurrences. If no filename or file extension is used we
    * assume the language to be 'unknown'.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing tuples indicating the programming language (extension) and number of occurrences.
    */
  def assignment_2(commits: RDD[Commit]): RDD[(String, Long)] = {

    val filenameMap = commits.flatMap(_.files.map(x => (x.filename match {
      case Some(fname) if (fname.contains(".")) => fname.split("\\.").last
      case Some(fname) if !(fname.contains(".")) => "unknown"
      case None => "unknown"
    }, 1L)))

    val res = filenameMap.reduceByKey(_ + _)

    res

  }

  /**
    * Competitive users on GitHub might be interested in their ranking in the number of commits. We want you to return an
    * RDD containing Tuples of the rank (zero indexed) of a commit author, a commit author's name and the number of
    * commits made by the commit author. As in general with performance rankings, a higher performance means a better
    * ranking (0 = best). In case of a tie, the lexicographical ordering of the usernames should be used to break the
    * tie.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing the rank, the name and the total number of commits for every author, in the ordered fashion.
    */
  def assignment_3(commits: RDD[Commit]): RDD[(Long, String, Long)] = {
    val result = commits.map(commits => (commits.commit.author.name, 1L)).reduceByKey((acc, x) => acc + x).sortBy(x => (-x._2, x._1.toLowerCase), true).zipWithIndex()
    val result1 = result.map((x) => (x._2, x._1._1, x._1._2))
    result1
  }

  /**
    * Some users are interested in seeing an overall contribution of all their work. For this exercise we want an RDD that
    * contains the committer's name and the total number of their commit statistics. As stats are Optional, missing Stats cases should be
    * handled as "Stats(0, 0, 0)".
    *
    * Note that if a user is given that is not in the dataset, then the user's name should not occur in
    * the resulting RDD.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing committer names and an aggregation of the committers Stats.
    */
  def assignment_4(commits: RDD[Commit], users: List[String]): RDD[(String, Stats)] = {
    val  cmap = commits.map(x => (x.commit.committer.name, x.stats.getOrElse(Stats(0, 0, 0))))
    val filt = cmap.filter(x => users.contains(x._1))
    val res = filt.reduceByKey((a, b) => Stats(a.total + b.total, a.additions + b.additions, a.deletions + b.deletions))
    res
  }

  /**
    * There are different types of people: those who own repositories, and those who make commits. Although Git blame command is
    * excellent in finding these types of people, we want to do it in Spark. As the output, we require an RDD containing the
    * names of commit authors and repository owners that have either exclusively committed to repositories, or
    * exclusively own repositories in the given commits RDD.
    *
    * Note that the repository owner is contained within GitHub URLs.
    *
    * @param commits RDD containing commit data.
    * @return RDD of Strings representing the usernames that have either only committed to repositories or only own
    *         repositories.
    */
def assignment_5(commits: RDD[Commit]): RDD[String] = {
    val spliturls = commits.map(x => x.url.split("/"))
    val owners = spliturls.map(x => x(4))
    val committers = commits.map(x => x.commit.author.name)

    val common = owners.intersection(committers)
    val combined = (owners.union(committers)).distinct()

    val res = combined.subtract(common)

    res
    }

  /**
    * Sometimes developers make mistakes and sometimes they make many many of them. One way of observing mistakes in commits is by
    * looking at so-called revert commits. We define a 'revert streak' as the number of times `Revert` occurs
    * in a commit message. Note that for a commit to be eligible for a 'revert streak', its message must start with `Revert`.
    * As an example: `Revert "Revert ...` would be a revert streak of 2, whilst `Oops, Revert Revert little mistake`
    * would not be a 'revert streak' at all.
    *
    * We require an RDD containing Tuples of the username of a commit author and a Tuple containing
    * the length of the longest 'revert streak' of a user and how often this streak has occurred.
    * Note that we are only interested in the longest commit streak of each author (and its frequency).
    *
    * @param commits RDD containing commit data.
    * @return RDD of Tuples containing a commit author's name and a Tuple which contains the length of the longest
    *         'revert streak' as well its frequency.
    */
  def assignment_6(commits: RDD[Commit]): RDD[(String, (Int, Int))] = {
    val commitsWithRevert = commits.filter(x => x.commit.message.startsWith("Revert"))
    val authorsWithCommits = commitsWithRevert.groupBy(x => x.commit.author.name)
    val authorsWithMessage = authorsWithCommits.map(x => (x._1, x._2.map(x => x.commit.message.split(" ").count(x => x.contains("Revert")))))
    authorsWithMessage.map(x => (x._1, (x._2.max, x._2.groupBy(identity).mapValues(_.size)(x._2.max))))
  }


  /**
    * !!! NOTE THAT FROM THIS EXERCISE ON (INCLUSIVE), EXPENSIVE FUNCTIONS LIKE groupBy ARE NO LONGER ALLOWED TO BE USED !!
    *
    * We want to know the number of commits that have been made to each repository contained in the given RDD. Besides the
    * number of commits, we also want to know the unique committers that contributed to each of these repositories.
    *
    * In real life these wide dependency functions are performance killers, but luckily there are better performing alternatives!
    * The automatic graders will check the computation history of the returned RDDs.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing Tuples with the repository name, the number of commits made to the repository as
    *         well as the names of the unique committers to this repository.
    */
  def assignment_7(commits: RDD[Commit]): RDD[(String, Long, Iterable[String])] = {

    val repNames = commits.map(x => (x.url.split("/")(5), List(x)))
    val repWithAmount = repNames.map(x => (x._1, x._2)).reduceByKey((x,y)=> x ++ y);
    val result = repWithAmount.map(x => (x._1, x._2.size.toLong, x._2.map(x => x.commit.committer.name).distinct));
    result.map(x => (x._1, x._2, x._3.toIterable));
  }

  /**
    * Return an RDD of Tuples containing the repository name and all the files that are contained in this repository.
    * Note that the file names must be unique, so if a file occurs multiple times (for example, due to removal, or new
    * addition), the newest File object must be returned. As the filenames are an `Option[String]`, discard the
    * files that do not have a filename.
    *
    * To reiterate, expensive functions such as groupBy are not allowed.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing the files in each repository as described above.
    */
  def assignment_8(commits: RDD[Commit]): RDD[(String, Iterable[File])] = {
    val repos = commits
      .map(i => (List(i), i.url.substring(i.url.indexOf("/repos/")+7, i.url.indexOf("/commits/")).split("/").apply(1)))
    val reposOptimized = repos
      .map(i => (i._2, i._1))
      .reduceByKey((x,y) => x.union(y))
      .map(i => (i._1, i._2.flatMap(_.files)))
    val result = reposOptimized
      .map(i => (i._1, i._2.filter(i => i.filename.isDefined)))
    val finalResult = result
      .map(i => (i._1, i._2.map {
      case file if ((i._2.count(k => k.filename.get.equals(file.filename.get))) > 1)
      => i._2.filter(l => l.filename.get.equals(file.filename.get)).maxBy(l => l.changes);
      case file if ((i._2.count(k => k.filename.get.equals(file.filename.get))) == 1)
      => file;
    }));
    finalResult.map(i => (i._1, i._2.distinct.toIterable));
  }



  /**
    * For this assignment you are asked to find all the files of a single repository. This is in order to create an
    * overview of each file by creating a Tuple containing the file name, all corresponding commit SHA's,
    * as well as a Stat object representing all the changes made to the file.
    *
    * To reiterate, expensive functions such as groupBy are not allowed.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing Tuples representing a file name, its corresponding commit SHA's and a Stats object
    *         representing the total aggregation of changes for a file.
    */
  def assignment_9(commits: RDD[Commit], repository: String): RDD[(String, Seq[String], Stats)] = {

    val commitsofrepo = commits.filter(x => x.url.contains(repository))
    val filesofrepo = commitsofrepo.flatMap(x => x.files) //tested correct
    val nameSHAchanges = filesofrepo.map(x => (x.filename.getOrElse(""), (Seq(x.sha.getOrElse("")), Stats(x.changes, x.additions, x.deletions)))) //, Stats(x.changes, x.additions, x.deletions)
    val totalSHAs = nameSHAchanges.reduceByKey((a,b) => (a._1.union(b._1) , Stats(a._2.total + b._2.total, a._2.additions + b._2.additions, a._2.deletions + b._2.deletions)))
    val res = totalSHAs.map(x => (x._1, x._2._1, x._2._2))

    res
  }

  /**
    * We want to generate an overview of the work done by a user per repository. For this we want an RDD containing
    * Tuples with the committer's name, the repository name and a `Stats` object containing the
    * total number of additions, deletions and total contribution to this repository.
    * Note that since Stats are optional, the required type is Option[Stat].
    *
    * To reiterate, expensive functions such as groupBy are not allowed.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing Tuples of the committer's name, the repository name and an `Option[Stat]` object representing additions,
    *         deletions and the total contribution to this repository by this committer.
    */
  def assignment_10(commits: RDD[Commit]): RDD[(String, String, Option[Stats])] =
  {
    val result = commits.map(x => ((x.url.split("/")(5), x.commit.committer.name), x.stats)).reduceByKey((x, y) =>
      Option(Stats(x.get.total + y.get.total, x.get.additions + y.get.additions, x.get.deletions + y.get.deletions)))
    result.map(x => (x._1._2, x._1._1, x._2))
  }


  /**
    * Hashing function that computes the md5 hash of a String and returns a Long, representing the most significant bits of the hashed string.
    * It acts as a hashing function for repository name and username.
    *
    * @param s String to be hashed, consecutively mapped to a Long.
    * @return Long representing the MSB of the hashed input String.
    */
  def md5HashString(s: String): Long = {
    val md = MessageDigest.getInstance("MD5")
    val digest = md.digest(s.getBytes)
    val bigInt = new BigInteger(1, digest)
    val hashedString = bigInt.toString(16)
    UUID.nameUUIDFromBytes(hashedString.getBytes()).getMostSignificantBits
  }

  /**
    * Create a bi-directional graph from committer to repositories. Use the `md5HashString` function above to create unique
    * identifiers for the creation of the graph.
    *
    * Spark's GraphX library is actually used in the real world for algorithms like PageRank, Hubs and Authorities, clique finding, etc.
    * However, this is out of the scope of this course and thus, we will not go into further detail.
    *
    * We expect a node for each repository and each committer (based on committer name).
    * We expect an edge from each committer to the repositories that they have committed to.
    *
    * Look into the documentation of Graph and Edge before starting with this exercise.
    * Your vertices must contain information about the type of node: a 'developer' or a 'repository' node.
    * Edges must only exist between repositories and committers.
    *
    * To reiterate, expensive functions such as groupBy are not allowed.
    *
    * @param commits RDD containing commit data.
    * @return Graph representation of the commits as described above.
    */
  def assignment_11(commits: RDD[Commit]): Graph[(String, String), String] =
  {
    val repos = commits.map(x => (x.commit.committer.name, x.url.substring(x.url.indexOf("/repos/")+7, x.url.indexOf("/commits/")), "repository"))
    val committers = commits.map(x => (x.url.substring(x.url.indexOf("/repos/")+7, x.url.indexOf("/commits/")), x.commit.committer.name, "developer"))
    val helper = repos.union(committers).map(x => (x._1, md5HashString(x._1), x._2, md5HashString(x._2), x._3)).filter(x => x._5.equals("developer")).distinct()
    val edges = helper.map(x => Edge(x._2, x._4, x._5))
    val vertices = helper.map(x => (x._2, (x._1, x._5)))
    val graph = Graph(vertices, edges)
    graph
  }
}
