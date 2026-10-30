package Generator.DatasetJob

import Generator.{AbstractJob, AbstractWayangJob, DatasetHelper}
import Generator.DatasetJob.imdb_kaggle.table.IMDBKDatasetOperatorManager
import Generator.DatasetJob.job.JOBDatasetOperatorManager
import Generator.DatasetJob.tpc_h.table.TPCHDatasetOperatorManager
import Generator.DatasetJob.utils.getElementBySeed

import scala.collection.mutable
import scala.util.Random

/**
  * Created by researchuser7 on 2020-04-20.
  */

/**
  * An equi-join between two tables: `lx.lxField = rx.field`.
  *
  * `field` is the join column of the right-hand table `rx`; `lxField` is the join column of the
  * left-hand table `lx`. In TPC-H both sides always use the same column name (e.g. ORDERKEY), so
  * the three-argument constructor sets `lxField = field`. Schemas like JOB/IMDB join columns with
  * different names (e.g. `cast_info.movie_id = title.id`) and need the four-argument form.
  */
case class JoinRelation(lx: AbstractTableOperatorManager, rx: AbstractTableOperatorManager, field: String, lxField: String) {
  override def equals(joinRelation: Any): Boolean = {
    joinRelation match {
      case jr: JoinRelation =>
        ((lx == jr.lx) && (rx == jr.rx) && (lxField == jr.lxField) && (field == jr.field)) ||
          ((lx == jr.rx) && (rx == jr.lx) && (lxField == jr.field) && (field == jr.lxField))
      case _ => false
    }
  }

  override def hashCode(): Int = Set((lx.tableName, lxField), (rx.tableName, field)).hashCode()

  override def toString: String =
    if (lxField == field) f"$lx%10s - $rx%10s : $field%10s "
    else f"$lx%10s - $rx%10s : $lxField%10s = $field%-10s "
}

object JoinRelation {
  /** Join on a column that has the same name in both tables (the TPC-H convention). */
  def apply(lx: AbstractTableOperatorManager, rx: AbstractTableOperatorManager, field: String): JoinRelation =
    JoinRelation(lx, rx, field, field)
}

trait AbstractDatasetOperatorManager {

  val tables: Seq[String]

  var platform : String = "Flink"

  def getTableOperatorManager(s: String,p: String): AbstractTableOperatorManager

  /**
    * Wraps the generated operator code into a Wayang job. Datasets that need extra definitions in
    * the generated file (e.g. their own CSV parsers) override this.
    *
    * @param tables names of the tables the job reads
    */
  def wayangJob(jobBody: String, jobName: String, saveExecutionPlan: Boolean, tables: Seq[String]): AbstractJob =
    AbstractWayangJob(jobBody, jobName, saveExecutionPlan = saveExecutionPlan)

  /**
    * Wraps the generated operator code into a Spark job, including the parsers for `tables`.
    * Only datasets that implement Spark code generation override this.
    */
  def sparkJob(jobBody: String, jobName: String, tables: Seq[AbstractTableOperatorManager]): AbstractJob =
    throw new Exception(s"""TARGET_PLATFORM "Spark" is not supported by ${getClass.getSimpleName}""")


  val maxAttempts = 100
  val random: Random.type = Random

  private def getNextJoinRelation(t0_OpMan: AbstractTableOperatorManager): JoinRelation = {

    // Get random join field
    var t0_jField = getElementBySeed(t0_OpMan.joinFields, this.random.nextInt(20)).toString

    // Get random join table
    var t0_jTable = getElementBySeed(t0_OpMan.joinFieldTable(t0_jField).keys.toSeq, this.random.nextInt(20)).toString

    //Get join table field
    var t0_jTableField = t0_OpMan.joinFieldTable(t0_jField)(t0_jTable)

    var t1_OpMan = getTableOperatorManager(t0_jTable,platform)

    JoinRelation(t0_OpMan, t1_OpMan, t0_jTableField, t0_jField)
  }

  def getSourceTableSequence(nJoins: Int, seed: Int = 0): (Seq[AbstractTableOperatorManager], Seq[JoinRelation]) = {
    var success = false
    var tmp_seed = seed
    var attempt = 0
    var tS:Seq[AbstractTableOperatorManager]=null
    var jS:Seq[JoinRelation]=null

    while (!success && (attempt <= 10)) {
      try {
        val (resTS, resJS) = _getSourceTableSequence(nJoins, tmp_seed)
        tS = resTS
        jS = resJS
        success = true
      } catch {
        case e: Exception =>
          attempt+=1
          success = false
          tmp_seed += 1
      }
    }
    (tS, jS)
  }

  def _getSourceTableSequence(nJoins: Int, seed: Int = 0): (Seq[AbstractTableOperatorManager], Seq[JoinRelation]) = {

    this.random.setSeed(seed)

    var tableSequence: mutable.Seq[AbstractTableOperatorManager] = mutable.Seq()
    var joinSequence: mutable.Seq[JoinRelation] = mutable.Seq()

    // Init table
    var t0 = getElementBySeed(tables, seed).toString
    var t0_OpMan = getTableOperatorManager(t0,platform)

    tableSequence = tableSequence :+ t0_OpMan

    for (i <- 0 until nJoins) {
      var nextJoinRelation: JoinRelation = null
      var attempt = 0

      do {
        attempt += 1

        nextJoinRelation = getNextJoinRelation(t0_OpMan)
      } while (joinSequence.contains(nextJoinRelation) && (attempt < maxAttempts))

      if (attempt >= maxAttempts) {
        throw new Exception(s"Impossible to generate '${nJoins}' join sequence for ${tableSequence}.")
      }

      joinSequence = joinSequence :+ nextJoinRelation
      t0_OpMan = nextJoinRelation.rx
      tableSequence = tableSequence :+ t0_OpMan
    }

    (tableSequence, joinSequence)
  }

}

object AbstractDatasetOperatorManager {
  def apply(s: String, p: String): AbstractDatasetOperatorManager = {
    if (s == "TPCH")
      new TPCHDatasetOperatorManager(p)
    else if (s == "IMDB" && p == "Spark")
      throw new Exception("DATA_MANAGER \"IMDB\" does not support TARGET_PLATFORM \"Spark\"")
    else if (s == "IMDB")
      IMDBKDatasetOperatorManager
    else if (s == "JOB")
      new JOBDatasetOperatorManager(p)
    else
      throw new Exception(s"Can not find Dataset Operator Manger '$s'.")
  }
}
