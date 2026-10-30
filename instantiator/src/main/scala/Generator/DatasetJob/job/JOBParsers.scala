package Generator.DatasetJob.job

/** Parse functions for the IMDB CSV files, generated into each JOB job (Wayang and Spark). */
object JOBParsers {

  /**
    * Scala source of an object with one parse function per table (named like the table) plus
    * the helpers used by generated filters. CSV dialect of the IMDB dump: `,` separated, `"`
    * quotes, `\` escapes inside quotes (`""` is accepted too), empty unquoted field = NULL.
    * Rows with missing trailing fields get NULLs; unparsable integers become NULL.
    */
  def code(objectName: String, tables: Seq[String]): String = {
    val parsers = tables.distinct.sorted.map(JOBSchema.table).map { t =>
      val values = t.columns.zipWithIndex.map { case (c, i) => if (c.isInt) s"int(f($i))" else s"f($i)" }
      s"  def ${t.name}(line: String): ${t.tupleType} = {\n" +
        s"    val f = split(line, ${t.columns.size})\n" +
        s"    ${values.mkString("(", ", ", ")")}\n" +
        "  }"
    }

    (Seq(
      s"/** IMDB CSV parsing and predicate helpers for [[$objectName]]. */",
      s"object $objectName {",
      "",
      "  /** NULL integers are represented as Long.MinValue, NULL text as null. */",
      "  val NullInt: Long = Long.MinValue",
      "",
      "  def int(s: String): Long =",
      "    if (s == null || s.isEmpty) NullInt",
      "    else try s.trim.toLong catch { case _: NumberFormatException => NullInt }",
      "",
      "  /** Splits a CSV line into exactly n fields. */",
      "  def split(line: String, n: Int): Array[String] = {",
      "    val out = new Array[String](n)",
      "    val len = if (line.endsWith(\"\\r\")) line.length - 1 else line.length",
      "    val sb = new java.lang.StringBuilder",
      "    var i = 0",
      "    var col = 0",
      "    while (col < n && i <= len) {",
      "      if (i < len && line.charAt(i) == '\"') {",
      "        sb.setLength(0)",
      "        i += 1",
      "        var open = true",
      "        while (open && i < len) {",
      "          val c = line.charAt(i)",
      "          if (c == '\\\\' && i + 1 < len) { sb.append(line.charAt(i + 1)); i += 2 }",
      "          else if (c == '\"' && i + 1 < len && line.charAt(i + 1) == '\"') { sb.append('\"'); i += 2 }",
      "          else if (c == '\"') { open = false; i += 1 }",
      "          else { sb.append(c); i += 1 }",
      "        }",
      "        while (i < len && line.charAt(i) != ',') i += 1",
      "        out(col) = sb.toString",
      "      } else {",
      "        val j = line.indexOf(',', i)",
      "        val end = if (j < 0 || j > len) len else j",
      "        out(col) = if (end == i) null else line.substring(i, end)",
      "        i = end",
      "      }",
      "      col += 1",
      "      i += 1",
      "    }",
      "    out",
      "  }",
      "",
      "  /** SQL LIKE: '%' matches any sequence, '_' any single character. */",
      "  def like(s: String, p: String): Boolean = {",
      "    var si = 0",
      "    var pi = 0",
      "    var starP = -1",
      "    var starS = 0",
      "    while (si < s.length) {",
      "      if (pi < p.length && p.charAt(pi) == '%') { starP = pi; starS = si; pi += 1 }",
      "      else if (pi < p.length && (p.charAt(pi) == '_' || p.charAt(pi) == s.charAt(si))) { si += 1; pi += 1 }",
      "      else if (starP >= 0) { pi = starP + 1; starS += 1; si = starS }",
      "      else return false",
      "    }",
      "    while (pi < p.length && p.charAt(pi) == '%') pi += 1",
      "    pi == p.length",
      "  }",
      ""
    ) ++ parsers :+ "}").mkString("\n")
  }
}
