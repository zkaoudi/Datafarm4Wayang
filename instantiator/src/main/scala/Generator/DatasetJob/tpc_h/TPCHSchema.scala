package Generator.DatasetJob.tpc_h

/**
  * TPC-H schema used to generate the CSV parsers of Wayang jobs.
  *
  * Generated TPC-H jobs read the dbgen `.tbl` files (`|` separated, trailing `|`) and parse each
  * line into a tuple with one element per column, in `.tbl` column order, so tuple position `i`
  * (1-based) is the column at position `i` in the table managers' `fields` maps. Column types in
  * the tuples:
  *
  *  - identifiers and integers: `Int`
  *  - decimals: `Double`
  *  - dates: `Int` in the form yyyymmdd (e.g. 19950619), so date filters compare integers
  *  - text: `String`
  *
  * The parsers live in an object `<jobName>Parsers` in the job file, so generated jobs need no
  * TPC-H classes on the Wayang side.
  */
object TPCHSchema {

  /** Placeholder for the parsers object in generated operator code; replaced per job. */
  val ParsersToken = "__TPCH_PARSERS__"

  sealed trait ColumnType { def scalaType: String; def parse(field: String): String }
  case object IntCol extends ColumnType { val scalaType = "Int"; def parse(f: String) = s"$f.trim.toInt" }
  case object DecimalCol extends ColumnType { val scalaType = "Double"; def parse(f: String) = s"$f.trim.toDouble" }
  case object DateCol extends ColumnType { val scalaType = "Int"; def parse(f: String) = s"date($f)" }
  case object TextCol extends ColumnType { val scalaType = "String"; def parse(f: String) = f }

  /** Columns of each table in .tbl order. */
  val columns: Map[String, Seq[(String, ColumnType)]] = Map(
    "region" -> Seq("REGIONKEY" -> IntCol, "NAME" -> TextCol, "COMMENT" -> TextCol),
    "nation" -> Seq("NATIONKEY" -> IntCol, "NAME" -> TextCol, "REGIONKEY" -> IntCol, "COMMENT" -> TextCol),
    "supplier" -> Seq("SUPPKEY" -> IntCol, "NAME" -> TextCol, "ADDRESS" -> TextCol, "NATIONKEY" -> IntCol,
      "PHONE" -> TextCol, "ACCTBAL" -> DecimalCol, "COMMENT" -> TextCol),
    "customer" -> Seq("CUSTKEY" -> IntCol, "NAME" -> TextCol, "ADDRESS" -> TextCol, "NATIONKEY" -> IntCol,
      "PHONE" -> TextCol, "ACCTBAL" -> DecimalCol, "MKTSEGMENT" -> TextCol, "COMMENT" -> TextCol),
    "part" -> Seq("PARTKEY" -> IntCol, "NAME" -> TextCol, "MFGR" -> TextCol, "BRAND" -> TextCol, "TYPE" -> TextCol,
      "SIZE" -> IntCol, "CONTAINER" -> TextCol, "RETAILPRICE" -> DecimalCol, "COMMENT" -> TextCol),
    "partsupp" -> Seq("PARTKEY" -> IntCol, "SUPPKEY" -> IntCol, "AVAILQTY" -> IntCol, "SUPPLYCOST" -> DecimalCol,
      "COMMENT" -> TextCol),
    "orders" -> Seq("ORDERKEY" -> IntCol, "CUSTKEY" -> IntCol, "ORDERSTATUS" -> TextCol, "TOTALPRICE" -> DecimalCol,
      "ORDERDATE" -> DateCol, "ORDERPRIORITY" -> TextCol, "CLERK" -> TextCol, "SHIPPRIORITY" -> IntCol,
      "COMMENT" -> TextCol),
    "lineitem" -> Seq("ORDERKEY" -> IntCol, "PARTKEY" -> IntCol, "SUPPKEY" -> IntCol, "LINENUMBER" -> IntCol,
      "QUANTITY" -> DecimalCol, "EXTENDEDPRICE" -> DecimalCol, "DISCOUNT" -> DecimalCol, "TAX" -> DecimalCol,
      "RETURNFLAG" -> TextCol, "LINESTATUS" -> TextCol, "SHIPDATE" -> DateCol, "COMMITDATE" -> DateCol,
      "RECEIPTDATE" -> DateCol, "SHIPINSTRUCT" -> TextCol, "SHIPMODE" -> TextCol, "COMMENT" -> TextCol)
  )

  def tupleType(table: String): String = columns(table).map(_._2.scalaType).mkString("(", ", ", ")")

  /** A yyyy-MM-dd date as the yyyymmdd integer used in the generated tuples. */
  def dateLiteral(date: String): String = date.replace("-", "")

  /**
    * Checks that a table manager's 1-based `fields` positions agree with the column order here,
    * so tuple positions in generated code point at the intended columns.
    */
  def checkFields(table: String, fields: Map[String, String]): Unit = {
    val cols = columns.getOrElse(table, throw new Exception(s"No TPC-H schema for table '$table'"))
    fields.foreach { case (name, pos) =>
      val i = pos.toInt
      if (i < 1 || i > cols.size || cols(i - 1)._1 != name)
        throw new Exception(s"TPC-H field $table.$name has position $pos, but the parser puts " +
          s"${if (i >= 1 && i <= cols.size) cols(i - 1)._1 else "nothing"} there")
    }
  }

  /** Scala source of the parsers object for the given tables (one parse function per table). */
  def parsersCode(objectName: String, tables: Seq[String]): String = {
    val parsers = tables.distinct.sorted.map { t =>
      val values = columns(t).zipWithIndex.map { case ((_, c), i) => c.parse(s"f($i)") }
      s"  def $t(line: String): ${tupleType(t)} = {\n" +
        s"    val f = line.split('|')\n" +
        s"    ${values.mkString("(", ", ", ")")}\n" +
        "  }"
    }
    (Seq(
      s"/** TPC-H .tbl parsing for [[$objectName]]: one function per table, fields in .tbl order. */",
      s"object $objectName {",
      "",
      "  /** yyyy-MM-dd as the integer yyyymmdd. */",
      "  def date(s: String): Int = {",
      "    val d = s.trim",
      "    d.substring(0, 4).toInt * 10000 + d.substring(5, 7).toInt * 100 + d.substring(8, 10).toInt",
      "  }",
      ""
    ) ++ parsers :+ "}").mkString("\n")
  }
}
