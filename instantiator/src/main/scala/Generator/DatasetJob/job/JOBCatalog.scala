package Generator.DatasetJob.job

import java.io.{File, FileInputStream, InputStream}

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import scala.collection.JavaConverters._

/**
  * A filter predicate on one column of a JOB table, e.g. `title.production_year > 2000`.
  *
  * Supported operators (SQL semantics: a NULL value never satisfies a predicate, except `is null`):
  *  - `=`, `!=`, `<`, `<=`, `>`, `>=` with one value (text columns compare lexicographically)
  *  - `between` with two values (inclusive)
  *  - `in` with one or more values
  *  - `like`, `not like` with one SQL pattern (`%` and `_`, no escape character); text columns only
  *  - `is null`, `is not null` without values
  *  - `mod` with two values `[m, r]`: `column % m == r`; integer columns only. Used for tables
  *    that are never filtered directly in JOB (their ids are dense, so `id % m == r` keeps ~1/m).
  *
  * @param selectivity fraction of the table's rows that satisfy the predicate, if known
  */
case class JOBPredicate(table: String, field: String, op: String, values: Seq[String], selectivity: Option[Double]) {

  import JOBPredicate._

  private val column: JOBColumn = JOBSchema.table(table).column(field)

  require(Ops.contains(op), s"$this: unsupported operator '$op' (supported: ${Ops.mkString(", ")})")
  require(arityOk, s"$this: wrong number of values for '$op'")
  require(!(column.isInt && TextOnly.contains(op)), s"$this: '$op' needs a text column")
  require(!(!column.isInt && IntOnly.contains(op)), s"$this: '$op' needs an integer column")
  if (column.isInt) values.foreach(v => require(scala.util.Try(v.toLong).isSuccess, s"$this: '$v' is not an integer"))

  private def arityOk: Boolean = op match {
    case "is null" | "is not null" => values.isEmpty
    case "between" | "mod" => values.size == 2
    case "in" => values.nonEmpty
    case _ => values.size == 1
  }

  /** Scala boolean expression over tuple variable `x`, e.g. `(x._5 != Long.MinValue && x._5 > 2000L)`. */
  def condition(parsersObject: String): String = {
    val v = s"x._${JOBSchema.table(table).index(field)}"
    if (column.isInt) {
      val notNull = s"$v != $IntNull"
      def l(s: String) = s"${s.trim.toLong}L"
      op match {
        case "=" => s"($notNull && $v == ${l(values.head)})"
        case "!=" | "<" | "<=" | ">" | ">=" => s"($notNull && $v $op ${l(values.head)})"
        case "between" => s"($notNull && $v >= ${l(values(0))} && $v <= ${l(values(1))})"
        case "in" => s"($v match { case ${values.map(l).mkString(" | ")} => true; case _ => false })"
        case "mod" => s"($notNull && $v % ${l(values(0))} == ${l(values(1))})"
        case "is null" => s"($v == $IntNull)"
        case "is not null" => s"($notNull)"
      }
    } else {
      val notNull = s"$v != null"
      op match {
        case "=" => s"($notNull && $v == ${lit(values.head)})"
        case "!=" => s"($notNull && $v != ${lit(values.head)})"
        case "<" | "<=" | ">" | ">=" => s"($notNull && $v.compareTo(${lit(values.head)}) $op 0)"
        case "between" => s"($notNull && $v.compareTo(${lit(values(0))}) >= 0 && $v.compareTo(${lit(values(1))}) <= 0)"
        case "in" => s"($v match { case ${values.map(lit).mkString(" | ")} => true; case _ => false })"
        case "like" => s"($notNull && $parsersObject.like($v, ${lit(values.head)}))"
        case "not like" => s"($notNull && !$parsersObject.like($v, ${lit(values.head)}))"
        case "is null" => s"($v == null)"
        case "is not null" => s"($notNull)"
      }
    }
  }

  /** Human-readable form, e.g. `title.production_year > 2000` (used in the job info JSON). */
  def sql: String = op match {
    case "is null" | "is not null" => s"$table.$field $op"
    case "between" => s"$table.$field between ${q(values(0))} and ${q(values(1))}"
    case "in" => s"$table.$field in (${values.map(q).mkString(", ")})"
    case "mod" => s"$table.$field % ${values(0)} = ${values(1)}"
    case _ => s"$table.$field $op ${q(values.head)}"
  }

  /**
    * Standard SQL condition on the bare column, e.g. `production_year > 2000`. SQL's NULL semantics
    * match the generated dataflow code (NULL never satisfies a predicate except `is null`).
    */
  def sqlCondition: String = op match {
    case "is null" => s"$field IS NULL"
    case "is not null" => s"$field IS NOT NULL"
    case "between" => s"$field BETWEEN ${q(values(0))} AND ${q(values(1))}"
    case "in" => s"$field IN (${values.map(q).mkString(", ")})"
    case "mod" => s"$field % ${values(0)} = ${values(1)}"
    case "like" => s"$field LIKE ${q(values.head)}"
    case "not like" => s"$field NOT LIKE ${q(values.head)}"
    case "!=" => s"$field <> ${q(values.head)}"
    case _ => s"$field $op ${q(values.head)}"
  }

  private def q(s: String) = if (column.isInt) s else "'" + s.replace("'", "''") + "'"

  override def toString: String = s"JOBPredicate($table.$field $op ${values.mkString("[", ", ", "]")})"
}

object JOBPredicate {
  val Ops: Seq[String] = Seq("=", "!=", "<", "<=", ">", ">=", "between", "in", "like", "not like", "is null", "is not null", "mod")
  val TextOnly: Set[String] = Set("like", "not like")
  val IntOnly: Set[String] = Set("mod")

  /** How NULL integers are represented in the generated tuples. */
  val IntNull = "Long.MinValue"

  /** Scala string literal for `s`. */
  def lit(s: String): String = {
    val sb = new StringBuilder("\"")
    s.foreach {
      case '\\' => sb.append("\\\\")
      case '"' => sb.append("\\\"")
      case '\n' => sb.append("\\n")
      case '\r' => sb.append("\\r")
      case '\t' => sb.append("\\t")
      case c if c < ' ' => sb.append(f"\\u${c.toInt}%04x")
      case c => sb.append(c)
    }
    sb.append('"').toString
  }
}

/** Statistics of one table: row count, distinct values of the group fields and filter predicates. */
case class JOBTableStats(rows: Option[Long], distinct: Map[String, Long], filters: Seq[JOBPredicate])

/**
  * Loads `job/job_catalog.json`: the filter predicates offered for each JOB table together with
  * their selectivities, distinct counts of group fields and table cardinalities.
  *
  * The predicates are taken from the 113 JOB queries. Statistics that depend on the actual IMDB
  * snapshot are `null` in the shipped catalog and are filled in by
  * `generator_labeler/DatasetMetadata/JOB_MetaDataAnalyzer.py`. Set the environment variable
  * `DATAFARM_JOB_CATALOG` to use a catalog file other than the bundled resource.
  */
object JOBCatalog {

  val ResourcePath = "/job/job_catalog.json"
  val EnvVar = "DATAFARM_JOB_CATALOG"

  def load(): Map[String, JOBTableStats] = {
    val (in, origin) = sys.env.get(EnvVar) match {
      case Some(path) => (new FileInputStream(new File(path)): InputStream, path)
      case None =>
        val s = getClass.getResourceAsStream(ResourcePath)
        if (s == null) throw new Exception(s"Can not find JOB catalog resource '$ResourcePath' on the classpath")
        (s, s"classpath:$ResourcePath")
    }
    try parse(new ObjectMapper().readTree(in), origin) finally in.close()
  }

  def parse(root: JsonNode, origin: String): Map[String, JOBTableStats] = {
    val tablesNode = root.get("tables")
    if (tablesNode == null) throw new Exception(s"JOB catalog $origin has no 'tables' entry")

    val unknown = tablesNode.fieldNames().asScala.toSeq.filterNot(JOBSchema.byName.contains)
    if (unknown.nonEmpty) throw new Exception(s"JOB catalog $origin lists unknown tables: ${unknown.mkString(", ")}")

    JOBSchema.tables.map { t =>
      val tn = Option(tablesNode.get(t.name)).getOrElse(throw new Exception(s"JOB catalog $origin has no entry for table '${t.name}'"))

      val distinctNode = Option(tn.get("distinct"))
      val distinct = distinctNode.map(_.fields().asScala.toSeq).getOrElse(Seq()).flatMap { e =>
        if (!t.groupFields.contains(e.getKey))
          throw new Exception(s"JOB catalog $origin: '${t.name}.${e.getKey}' is not a group field (${t.groupFields.mkString(", ")})")
        optLong(e.getValue).map(e.getKey -> _)
      }.toMap

      val filters = Option(tn.get("filters")).map(_.elements().asScala.toSeq).getOrElse(Seq()).map { f =>
        val field = text(f, "field", t.name)
        val op = text(f, "op", t.name).toLowerCase
        val values = Option(f.get("value")) match {
          case None => Seq()
          case Some(n) if n.isNull => Seq()
          case Some(n) if n.isArray => n.elements().asScala.map(_.asText()).toSeq
          case Some(n) => Seq(n.asText())
        }
        val sel = Option(f.get("selectivity")).filterNot(_.isNull).map(_.asDouble())
        JOBPredicate(t.name, field, op, values, sel)
      }
      if (filters.isEmpty) throw new Exception(s"JOB catalog $origin has no filters for table '${t.name}'")

      t.name -> JOBTableStats(optLong(tn.get("rows")), distinct, filters)
    }.toMap
  }

  private def optLong(n: JsonNode): Option[Long] = Option(n).filterNot(_.isNull).map(_.asLong())

  private def text(n: JsonNode, key: String, table: String): String =
    Option(n.get(key)).filterNot(_.isNull).map(_.asText()).getOrElse(
      throw new Exception(s"JOB catalog: a filter of table '$table' has no '$key'"))
}
