package Generator.DatasetJob.tpc_h

import Generator.DatasetJob.AbstractTableOperatorManager

/**
  * Parse functions for the TPC-H `.tbl` files (`|` separated), generated into each Spark TPC-H job.
  *
  * Tuple types follow each table manager's `typeSchema`, except that dates become `Int` in the
  * encoding of Wayang's `CsvUtils.parseDate` (year * 365 + month * 30 + day), which the generated
  * date filters compare against.
  */
object TPCHParsers {

  private def fieldTypes(typeSchema: String): Seq[String] =
    typeSchema.trim.stripPrefix("(").stripSuffix(")").split(",").map(_.trim).toSeq

  def code(objectName: String, tables: Seq[AbstractTableOperatorManager]): String = {
    val parsers = tables.groupBy(_.tableName).values.map(_.head).toSeq.sortBy(_.tableName).map { t =>
      val types = fieldTypes(t.typeSchema)
      val values = types.zipWithIndex.map {
        case ("Int", i) => s"f($i).trim.toInt"
        case ("Float", i) => s"f($i).trim.toFloat"
        case ("String", i) => s"f($i)"
        case ("java.sql.Date", i) => s"parseDate(f($i))"
        case (other, _) => throw new Exception(s"Unsupported TPC-H field type '$other' in table '${t.tableName}'")
      }
      val tupleType = types.map(tp => if (tp == "java.sql.Date") "Int" else tp).mkString("(", ", ", ")")
      s"  def ${t.tableName}(line: String): $tupleType = {\n" +
        "    val f = line.split('|')\n" +
        s"    ${values.mkString("(", ", ", ")")}\n" +
        "  }"
    }

    (Seq(
      s"/** TPC-H parsing helpers for [[$objectName]]. */",
      s"object $objectName {",
      "",
      "  /** yyyy-MM-dd as year * 365 + month * 30 + day (Wayang's CsvUtils.parseDate encoding). */",
      "  def parseDate(s: String): Int = {",
      "    val p = s.trim.split('-')",
      "    p(0).toInt * 365 + p(1).toInt * 30 + p(2).toInt",
      "  }",
      ""
    ) ++ parsers :+ "}").mkString("\n")
  }
}
