package Generator.DatasetJob.job

import Generator.{AbstractJob, AbstractSparkJob}
import Generator.DatasetJob.{AbstractDatasetOperatorManager, AbstractTableOperatorManager, JoinRelation}

/**
  * Dataset manager for the Join Order Benchmark (JOB) on the IMDB dataset.
  *
  * Generated jobs read the 21 IMDB CSV files from the job's `datapath` argument and target Wayang or
  * Spark (`TARGET_PLATFORM = "Wayang"` or `"Spark"`).
  */
class JOBDatasetOperatorManager(targetPlatform: String) extends AbstractDatasetOperatorManager {

  if (targetPlatform != "Wayang" && targetPlatform != "Spark")
    throw new Exception(s"""DATA_MANAGER "JOB" supports TARGET_PLATFORM "Wayang" and "Spark" (got "$targetPlatform").""")

  platform = targetPlatform

  val tables: Seq[String] = JOBSchema.tables.map(_.name)

  private val catalog: Map[String, JOBTableStats] = JOBCatalog.load()

  def getTableOperatorManager(s: String, p: String): AbstractTableOperatorManager = p match {
    case "Spark" => new JOBSparkTableOperatorManager(JOBSchema.table(s), catalog(s))
    case _ => new JOBWayangTableOperatorManager(JOBSchema.table(s), catalog(s))
  }

  override def wayangJob(jobBody: String, jobName: String, saveExecutionPlan: Boolean, tables: Seq[String]): AbstractJob =
    AbstractWayangJOBJob(jobBody, jobName, saveExecutionPlan, tables)

  override def sparkJob(jobBody: String, jobName: String, tables: Seq[AbstractTableOperatorManager]): AbstractJob = {
    val names = tables.map(_.tableName).distinct
    AbstractSparkJob(jobBody, jobName, obj => JOBParsers.code(obj, names), "JOB")
  }

  /**
    * Join sequence for `nJoins` joins: a walk over the JOB join graph where each join starts from
    * the table produced by the previous join and no join predicate is used twice.
    *
    * The default random walk gets stuck in JOB's lookup tables (kind_type, role_type, ...), whose
    * only join leads back to where the walk came from. Here the walk is a seeded, randomized
    * depth-first search that backs out of dead ends, so long sequences (JOB queries have up to 16
    * joins) are found whenever they exist. A table is used at most as often as it occurs in a
    * single JOB query ([[JOBSchema.maxOccurrences]]); only if no such sequence exists is that limit
    * dropped. The start table is chosen by the seed as before; if no sequence exists from it, the
    * other tables are tried.
    */
  override def getSourceTableSequence(nJoins: Int, seed: Int = 0): (Seq[AbstractTableOperatorManager], Seq[JoinRelation]) = {
    random.setSeed(seed)
    val first = tables(Math.floorMod(seed, tables.size))
    val starts = first +: random.shuffle(tables.filterNot(_ == first))

    // (table, its join column, other table, other table's join column)
    type Edge = (String, String, String, String)
    def key(e: Edge): Set[(String, String)] = Set((e._1, e._2), (e._3, e._4))
    def edgesFrom(t: String): Seq[Edge] =
      JOBSchema.joinFieldTable(t).toSeq.flatMap { case (f, targets) => targets.toSeq.map { case (ot, of) => (t, f, ot, of) } }

    def find(capped: Boolean): Option[(String, List[Edge])] = {
      var budget = 0
      def allowed(t: String, count: Map[String, Int]) = !capped || count.getOrElse(t, 0) < JOBSchema.maxOccurrences(t)
      def search(cur: String, used: Set[Set[(String, String)]], count: Map[String, Int], acc: List[Edge]): Option[List[Edge]] =
        if (acc.size == nJoins) Some(acc.reverse)
        else if (budget <= 0) None
        else {
          budget -= 1
          random.shuffle(edgesFrom(cur).filter(e => !used.contains(key(e)) && allowed(e._3, count)))
            .iterator
            .map(e => search(e._3, used + key(e), count.updated(e._3, count.getOrElse(e._3, 0) + 1), e :: acc))
            .collectFirst { case Some(p) => p }
        }
      starts.iterator.map { s => budget = 50000; s -> search(s, Set(), Map(s -> 1), Nil) }.collectFirst { case (s, Some(p)) => (s, p) }
    }

    val (start, path) = find(capped = true).orElse {
      println(s"WARN - no $nJoins-join sequence within JOB's table repetitions; allowing tables to repeat more often")
      find(capped = false)
    }.getOrElse(
      throw new Exception(s"No sequence of $nJoins joins without repeated join predicates exists in the JOB join graph"))

    val managers = (start +: path.map(_._3)).map(t => getTableOperatorManager(t, platform))
    val joins = path.zipWithIndex.map { case ((_, f, _, of), i) => JoinRelation(managers(i), managers(i + 1), of, f) }
    (managers, joins)
  }

  def main(args: Array[String]): Unit = {
    for (i <- 0 to 20) {
      val (tableSequence, joinSequence) = getSourceTableSequence(5, i)
      joinSequence.foreach(j => println(s"> $j"))
      println(s"Table sequence: $tableSequence")
      println("---------------------------------------------")
    }
  }
}

object JOBDatasetOperatorManager {
  /** Placeholder for the job-specific parser object; replaced when the job file is written. */
  val ParsersToken: String = Generator.DatasetJob.ParserCode.Token
}
