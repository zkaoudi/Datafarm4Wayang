package Generator.DatasetJob.job

import Generator.DatasetJob.AbstractWayangTableOperatorManager

import scala.collection.immutable.ListMap

/**
  * Generates Wayang operator code for one JOB table.
  *
  * Joins, maps, reduce and the sink come from [[AbstractWayangTableOperatorManager]]. This class
  * overrides the parts that depend on the JOB data: reading/parsing the CSV, filter predicates
  * from [[JOBCatalog]], and group-by/sort keys that are safe for nullable columns.
  */
class JOBTableOperatorManager(table: JOBTable, stats: JOBTableStats) extends AbstractWayangTableOperatorManager {

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
    s"""
       | val $outVarName = planBuilder
       | .readTextFile(datapath + "${table.fileName}")
       | .withName("Read ${table.name}")
       | .map(${JOBDatasetOperatorManager.ParsersToken}.${table.name})
       | .withName("Parse ${table.name} to tuple")
    """.stripMargin

  // ---- filter ------------------------------------------------------------------------------

  override def filterCode(inVarName: String, outVarName: String, fieldSeed: Int = 0, valueSeed: Int = 0): (String, Double) = {
    val p = predicates(Math.floorMod(fieldSeed, predicates.size))
    if (p.selectivity.isEmpty)
      println(s"WARN - no selectivity for '${p.sql}' in the JOB catalog; recording -1.0 (run JOB_MetaDataAnalyzer.py)")
    val code =
      s"""
         |val $outVarName = $inVarName.filter(x => ${p.condition(JOBDatasetOperatorManager.ParsersToken)})
      """.stripMargin
    (code, p.selectivity.getOrElse(-1.0))
  }

  // ---- group by ----------------------------------------------------------------------------

  /** Key extractor for a group field; NULL text values are grouped under "" (Wayang rejects null keys). */
  private def keyUdf(field: String): String = {
    val c = table.column(field)
    val i = table.index(field)
    if (!c.isInt && c.nullable) s"""(x => if (x._$i == null) "" else x._$i)""" else s"(_._$i)"
  }

  private def groupField(seed: Int): String = table.groupFields(Math.floorMod(seed, table.groupFields.size))

  override def groupByCode1(inVarName: String, outVarName: String, seed: Int): (String, Double) = {
    val g = groupField(seed)
    (s"""
        |val $outVarName = $inVarName.reduceByKey(${keyUdf(g)}, (t1, t2) => t1)
      """.stripMargin, groupFields(g).toDouble)
  }

  override def groupByCode2(inVarName: String, outVarName1: String, outVarName2: String, seed: Int): (String, Double) = {
    // two consecutive group-bys collapse into one reduce-by-key, as for TPC-H on Wayang
    val g = groupField(seed)
    (s"""
        |val $outVarName2 = $inVarName.reduceByKey(${keyUdf(g)}, (t1, t2) => t1)
      """.stripMargin, groupFields(g).toDouble)
  }

  override def reduceCode2(inVarName: String, outVarName1: String, outVarName2: String, seed: Int): String =
    s"""
       |val $outVarName2 = $inVarName.reduce((x1, x2) => x1)
      """.stripMargin

  // ---- sort --------------------------------------------------------------------------------

  override def sortPartitionCode(inVarName: String, outVarName: String, seed: Int): String = {
    val keys = table.nullSafeKeyColumns
    val c = keys(Math.floorMod(seed, keys.size))
    s"""
       |val $outVarName = $inVarName.sort(_._${table.index(c.name)})
      """.stripMargin
  }
}
