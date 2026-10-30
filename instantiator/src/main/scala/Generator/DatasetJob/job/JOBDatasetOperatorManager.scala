package Generator.DatasetJob.job

import Generator.AbstractJob
import Generator.DatasetJob.{AbstractDatasetOperatorManager, AbstractTableOperatorManager}

/**
  * Dataset manager for the Join Order Benchmark (JOB) on the IMDB dataset.
  *
  * Generated jobs read the 21 IMDB CSV files from the job's `datapath` argument and only target
  * Wayang (`TARGET_PLATFORM = "Wayang"`).
  */
class JOBDatasetOperatorManager(targetPlatform: String) extends AbstractDatasetOperatorManager {

  if (targetPlatform != "Wayang")
    throw new Exception(s"""DATA_MANAGER "JOB" only supports TARGET_PLATFORM "Wayang" (got "$targetPlatform").""")

  platform = targetPlatform

  val tables: Seq[String] = JOBSchema.tables.map(_.name)

  private val catalog: Map[String, JOBTableStats] = JOBCatalog.load()

  def getTableOperatorManager(s: String, p: String): AbstractTableOperatorManager =
    new JOBTableOperatorManager(JOBSchema.table(s), catalog(s))

  override def wayangJob(jobBody: String, jobName: String, saveExecutionPlan: Boolean, tables: Seq[String]): AbstractJob =
    AbstractWayangJOBJob(jobBody, jobName, saveExecutionPlan, tables)

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
  val ParsersToken = "__JOB_PARSERS__"
}
