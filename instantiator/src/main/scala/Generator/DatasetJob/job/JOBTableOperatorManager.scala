package Generator.DatasetJob.job

import Generator.DatasetJob.{AbstractSparkTableOperatorManager, AbstractTableOperatorManager, AbstractWayangTableOperatorManager, ParserCode}

import com.fasterxml.jackson.annotation.JsonIgnore

import scala.annotation.meta.{field, getter, param}

import scala.collection.immutable.ListMap

/**
  * The JOB-specific part of operator generation for one table, independent of the target platform:
  * reading/parsing the CSV, filter predicates from [[JOBCatalog]], and group-by/sort keys that are
  * safe for nullable columns.
  *
  * Mixed in after a platform trait ([[AbstractWayangTableOperatorManager]] or
  * [[AbstractSparkTableOperatorManager]]), which supplies joins, maps and the sink; the few
  * platform-specific code fragments used here come from the abstract hooks below.
  */
trait JOBTableLogic extends AbstractTableOperatorManager {

  // JobInfoRecorder serializes table managers: the schema is left out of the job info JSON, the stats are kept
  protected def table: JOBTable

  protected def stats: JOBTableStats

  // ---- platform hooks ----------------------------------------------------------------------

  /** Read `file` from `datapath` and turn each line into a tuple with `parser`. */
  protected def jobSourceCode(outVarName: String, file: String, parser: String, name: String): String

  /** Keep one element per key. */
  protected def jobKeyedReduceCode(inVarName: String, outVarName: String, keyUdf: String): String

  /** Reduce the whole input to a single element. */
  protected def jobGlobalReduceCode(inVarName: String, outVarName: String): String

  /** Sort by the tuple field at 1-based position `index`. */
  protected def jobSortCode(inVarName: String, outVarName: String, index: Int): String

  // ---- table description -------------------------------------------------------------------

  override val tableName: String = table.name

  override val wayangName: String = table.name

  override val typeSchema: String = table.tupleType

  override val fields: Map[String, String] =
    ListMap(table.columns.zipWithIndex.map { case (c, i) => c.name -> (i + 1).toString }: _*)

  override val joinFieldTable: Map[String, Map[String, String]] = JOBSchema.joinFieldTable(table.name)

  private val predicates: Seq[JOBPredicate] = stats.filters

  /** field -> predicates (as SQL) and their selectivities ("-1" = not measured yet); informational. */
  override val filterFieldValue: Map[String, Map[String, Seq[String]]] =
    ListMap(predicates.map(_.field).distinct.map { f =>
      val ps = predicates.filter(_.field == f)
      f -> Map("predicates" -> ps.map(_.sql), "selectivity" -> ps.map(p => fmt(p.selectivity)))
    }: _*)

  /** group field -> number of distinct values ("-1" = not measured yet). */
  override val groupFields: Map[String, String] =
    ListMap(table.groupFields.map(g => g -> stats.distinct.get(g).map(_.toString).getOrElse("-1")): _*)

  private def fmt(d: Option[Double]): String = d.map(_.toString).getOrElse("-1")

  // ---- data source -------------------------------------------------------------------------

  override def dataSourceCode(outVarName: String): String =
    jobSourceCode(outVarName, table.fileName, s"${ParserCode.Token}.${table.name}", table.name)

  // ---- filter ------------------------------------------------------------------------------

  override def filterCode(inVarName: String, outVarName: String, fieldSeed: Int = 0, valueSeed: Int = 0): (String, Double) = {
    val p = predicates(Math.floorMod(fieldSeed, predicates.size))
    if (p.selectivity.isEmpty)
      println(s"WARN - no selectivity for '${p.sql}' in the JOB catalog; recording -1.0 (run JOB_MetaDataAnalyzer.py)")
    val code =
      s"""
         |val $outVarName = $inVarName.filter(x => ${p.condition(ParserCode.Token)})
      """.stripMargin
    (code, p.selectivity.getOrElse(-1.0))
  }

  // ---- group by ----------------------------------------------------------------------------

  /** Key extractor for a group field; NULL text values are grouped under "" (null keys are rejected). */
  private def keyUdf(field: String): String = {
    val c = table.column(field)
    val i = table.index(field)
    if (!c.isInt && c.nullable) s"""(x => if (x._$i == null) "" else x._$i)""" else s"(_._$i)"
  }

  private def groupField(seed: Int): String = table.groupFields(Math.floorMod(seed, table.groupFields.size))

  override def groupByCode1(inVarName: String, outVarName: String, seed: Int): (String, Double) = {
    val g = groupField(seed)
    (jobKeyedReduceCode(inVarName, outVarName, keyUdf(g)), groupFields(g).toDouble)
  }

  override def groupByCode2(inVarName: String, outVarName1: String, outVarName2: String, seed: Int): (String, Double) = {
    // two consecutive group-bys collapse into one keyed reduce, as for TPC-H on Wayang
    val g = groupField(seed)
    (jobKeyedReduceCode(inVarName, outVarName2, keyUdf(g)), groupFields(g).toDouble)
  }

  // ---- reduce ------------------------------------------------------------------------------

  // In the JOB workload a Reduce pair is the combiner + reduce of the global MIN(...) aggregate.
  override def reduceCode1(inVarName: String, outVarName: String): String = jobGlobalReduceCode(inVarName, outVarName)

  override def reduceCode2(inVarName: String, outVarName1: String, outVarName2: String, seed: Int): String =
    jobGlobalReduceCode(inVarName, outVarName2)

  // ---- sort --------------------------------------------------------------------------------

  override def sortPartitionCode(inVarName: String, outVarName: String, seed: Int): String = {
    val keys = table.nullSafeKeyColumns
    val c = keys(Math.floorMod(seed, keys.size))
    jobSortCode(inVarName, outVarName, table.index(c.name))
  }
}

/** JOB table on Wayang. */
class JOBWayangTableOperatorManager(@(JsonIgnore @param @field @getter) protected val table: JOBTable, protected val stats: JOBTableStats)
  extends AbstractWayangTableOperatorManager with JOBTableLogic {

  protected def jobSourceCode(outVarName: String, file: String, parser: String, name: String): String =
    s"""
       | val $outVarName = planBuilder
       | .readTextFile(datapath + "$file")
       | .withName("Read $name")
       | .map($parser)
       | .withName("Parse $name to tuple")
    """.stripMargin

  protected def jobKeyedReduceCode(inVarName: String, outVarName: String, keyUdf: String): String =
    s"""
        |val $outVarName = $inVarName.reduceByKey($keyUdf, (t1, t2) => t1)
      """.stripMargin

  protected def jobGlobalReduceCode(inVarName: String, outVarName: String): String =
    s"""
       |val $outVarName = $inVarName.reduce((x1, x2) => x1)
      """.stripMargin

  protected def jobSortCode(inVarName: String, outVarName: String, index: Int): String =
    s"""
       |val $outVarName = $inVarName.sort(_._$index)
      """.stripMargin
}

/** JOB table on Spark (RDD API). */
class JOBSparkTableOperatorManager(@(JsonIgnore @param @field @getter) protected val table: JOBTable, protected val stats: JOBTableStats)
  extends AbstractSparkTableOperatorManager with JOBTableLogic {

  protected def jobSourceCode(outVarName: String, file: String, parser: String, name: String): String =
    sparkSourceCode(outVarName, file, parser, name)

  protected def jobKeyedReduceCode(inVarName: String, outVarName: String, keyUdf: String): String =
    sparkKeyedReduceCode(inVarName, outVarName, keyUdf)

  protected def jobGlobalReduceCode(inVarName: String, outVarName: String): String =
    sparkGlobalReduceCode(inVarName, outVarName)

  protected def jobSortCode(inVarName: String, outVarName: String, index: Int): String =
    sparkSortCode(inVarName, outVarName, index)
}
