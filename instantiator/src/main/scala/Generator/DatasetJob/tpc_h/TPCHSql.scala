package Generator.DatasetJob.tpc_h

import Generator.DatasetJob.{AbstractSQLTableOperatorManager, AbstractTableOperatorManager}

/** TPC-H column names as created by dbgen's `dss.ddl`: table prefix + lower-case field, e.g. `l_orderkey`. */
trait TPCHSqlNaming extends AbstractSQLTableOperatorManager {
  override def sqlColumn(field: String): String = TPCHSql.prefix(tableName) + field.toLowerCase
}

object TPCHSql {

  val prefix: Map[String, String] = Map(
    "lineitem" -> "l_", "orders" -> "o_", "customer" -> "c_", "part" -> "p_",
    "partsupp" -> "ps_", "supplier" -> "s_", "nation" -> "n_", "region" -> "r_")

  private def sqlType(scalaType: String, table: String): String = scalaType match {
    case "Int" => "INTEGER"
    case "Float" => "DECIMAL(15,2)"
    case "String" => "VARCHAR(199)"
    case "java.sql.Date" => "DATE"
    case other => throw new Exception(s"Unsupported TPC-H field type '$other' in table '$table'")
  }

  /** `CREATE TABLE` statements for the given TPC-H tables, columns in file order. */
  def ddl(tables: Seq[AbstractTableOperatorManager]): String = {
    val creates = tables.map { t =>
      val types = t.typeSchema.trim.stripPrefix("(").stripSuffix(")").split(",").map(_.trim)
      val names = t.fields.toSeq.sortBy(_._2.toInt).map(_._1)
      require(types.length == names.size, s"typeSchema and fields of '${t.tableName}' differ in length")
      val cols = names.zip(types).map { case (n, tp) => s"    ${t.sqlColumn(n)} ${sqlType(tp, t.tableName)} NOT NULL" }
      s"CREATE TABLE ${t.tableName} (\n${cols.mkString(",\n")}\n);"
    }
    s"""-- TPC-H tables used by the generated SQL jobs (column names as in dbgen's dss.ddl).
       |-- Load the .tbl files, e.g. in PostgreSQL after removing the trailing '|' of each line:
       |--   \\copy lineitem FROM 'lineitem.tbl' WITH (FORMAT csv, DELIMITER '|')
       |-- or in DuckDB:
       |--   COPY lineitem FROM 'lineitem.tbl' (DELIMITER '|', HEADER false, NULL_PADDING true);
       |
       |${creates.mkString("\n\n")}
       |""".stripMargin
  }
}
