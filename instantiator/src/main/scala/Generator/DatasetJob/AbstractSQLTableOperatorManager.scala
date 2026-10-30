package Generator.DatasetJob

import Generator.AbstractSQLJob
import Generator.DatasetJob.utils.getElementBySeed

/**
  * SQL code generation for a table: every abstract operator becomes one common table expression
  * (CTE), named like the variables of the dataflow jobs (`source0`, `filter1`, `join2`, ...), so a
  * job is one `WITH ... SELECT` statement with the same operators, in the same order, as the
  * Wayang, Flink and Spark jobs generated from the same plan:
  *
  *  - Data Source: `SELECT * FROM table`
  *  - Filter: `SELECT * FROM in WHERE condition`
  *  - Join: `SELECT r.* FROM lx l JOIN rx r ON l.a = r.b` (keeps the right-hand rows, like the dataflow jobs)
  *  - Group by / keyed Reduce: one row per key, `SELECT key, MIN(c) AS c, ... GROUP BY key`
  *  - global Reduce: one row, `SELECT MIN(c) AS c, ... HAVING COUNT(*) > 0` (no row for empty input)
  *  - Sort-Partition: `SELECT * FROM in ORDER BY column`
  *  - Map, Partition: pass-through (`SELECT * FROM in`); their UDF cost has no SQL counterpart
  *
  * Only standard SQL is used (tested with PostgreSQL and DuckDB). Columns are named by [[sqlColumn]].
  */
trait AbstractSQLTableOperatorManager extends AbstractTableOperatorManager {

  // ---- building blocks (also used by dataset-specific managers) -----------------------------

  /** SQL table name. */
  def sqlTable: String = tableName

  /** SQL columns of this table, in tuple order. */
  def sqlColumns: Seq[String] = fields.toSeq.sortBy(_._2.toInt).map(f => sqlColumn(f._1))

  def sqlCte(name: String, query: String): String = AbstractSQLJob.cte(name, query)

  def sqlSourceCode(outVarName: String): String = sqlCte(outVarName, s"SELECT * FROM $sqlTable")

  def sqlPassThroughCode(inVarName: String, outVarName: String): String = sqlCte(outVarName, s"SELECT * FROM $inVarName")

  def sqlFilterCode(inVarName: String, outVarName: String, condition: String): String =
    sqlCte(outVarName, s"SELECT * FROM $inVarName WHERE $condition")

  /** One row per value of `keyExpr` (normally the column `keyColumn` itself); the other columns keep their minimum. */
  def sqlKeyedReduceCode(inVarName: String, outVarName: String, keyColumn: String, keyExpr: String): String = {
    val select = sqlColumns.map(c => if (c == keyColumn && keyExpr == keyColumn) c else s"MIN($c) AS $c")
    sqlCte(outVarName, s"SELECT ${select.mkString(", ")} FROM $inVarName GROUP BY $keyExpr")
  }

  def sqlGlobalReduceCode(inVarName: String, outVarName: String): String =
    sqlCte(outVarName, s"SELECT ${sqlColumns.map(c => s"MIN($c) AS $c").mkString(", ")} FROM $inVarName HAVING COUNT(*) > 0")

  def sqlSortCode(inVarName: String, outVarName: String, column: String): String =
    sqlCte(outVarName, s"SELECT * FROM $inVarName ORDER BY $column")

  private def fieldName(index: String): String =
    fields.find(_._2 == index).map(_._1).getOrElse(throw new Exception(s"No field at position $index in table '$tableName'"))

  // ---- operators ----------------------------------------------------------------------------

  override def dataSourceCode(outVarName: String): String = sqlSourceCode(outVarName)

  override def buildDataSourceCode(schema: String, tableName: String, outVarName: String, delimiter: String = "|"): String =
    sqlSourceCode(outVarName)

  /** `filterField` is the 1-based field position, `filterValue` a Scala literal as written by the table managers. */
  override def buildFilterCode(inVarName: String, outVarName: String, filterField: String, filterValue: String, filterOp: String = "<="): String = {
    val column = sqlColumn(fieldName(filterField))
    val value = filterValue.trim match {
      case AbstractTableOperatorManager.ParseDate(date) => s"DATE '$date'"
      case v if v.startsWith("\"") && v.endsWith("\"") => "'" + v.substring(1, v.length - 1).replace("'", "''") + "'"
      case v => v
    }
    val op = filterOp match {
      case "==" => "="
      case "!=" => "<>"
      case o => o
    }
    sqlFilterCode(inVarName, outVarName, s"$column $op $value")
  }

  override def buildJoinCode(lxVarName: String, rxVarName: String, outVarName: String, joinRelation: JoinRelation): String = {
    val lxColumn = joinRelation.lx.sqlColumn(joinRelation.lxField)
    val rxColumn = joinRelation.rx.sqlColumn(joinRelation.field)
    sqlCte(outVarName, s"SELECT r.* FROM $lxVarName l JOIN $rxVarName r ON l.$lxColumn = r.$rxColumn")
  }

  override def joinCode(lxVarName: String, rxVarName: String, outVarName: String, joinRelation: JoinRelation): String =
    buildJoinCode(lxVarName, rxVarName, outVarName, joinRelation)

  override def mapCode(inVarName: String, outVarName: String, seed: Int = 0): (String, Int) =
    (sqlPassThroughCode(inVarName, outVarName), seed % 3)

  override def groupByCode1(inVarName: String, outVarName: String, seed: Int): (String, Double) = {
    val gField = getElementBySeed(groupFields.keys.toSeq, seed).toString
    val c = sqlColumn(gField)
    (sqlKeyedReduceCode(inVarName, outVarName, c, c), groupFields(gField).toDouble)
  }

  override def groupByCode2(inVarName: String, outVarName1: String, outVarName2: String, seed: Int): (String, Double) = {
    // two consecutive group-bys collapse into one, as for the dataflow platforms
    val gField = getElementBySeed(groupFields.keys.toSeq, seed).toString
    val c = sqlColumn(gField)
    (sqlKeyedReduceCode(inVarName, outVarName2, c, c), groupFields(gField).toDouble)
  }

  override def reduceCode1(inVarName: String, outVarName: String): String = sqlGlobalReduceCode(inVarName, outVarName)

  /** A Reduce pair (combiner + reduce of a grouped reduce in the input workload) on a group field. */
  override def reduceCode2(inVarName: String, outVarName1: String, outVarName2: String, seed: Int): String = {
    val rField = getElementBySeed(groupFields.keys.toSeq, seed).toString
    val c = sqlColumn(rField)
    sqlKeyedReduceCode(inVarName, outVarName2, c, c)
  }

  override def sortPartitionCode(inVarName: String, outVarName: String, seed: Int): String = {
    val sField = getElementBySeed(fields.keys.toSeq, seed).toString
    sqlSortCode(inVarName, outVarName, sqlColumn(sField))
  }

  override def partitionCode(inVarName: String, outVarName: String): String = sqlPassThroughCode(inVarName, outVarName)

  override def sinkCode(inVarName: String, outVarName: String): String = AbstractSQLJob.sink(inVarName)
}
