package Generator.DatasetJob

import Generator.DatasetJob.utils.getElementBySeed

/**
  * Spark (RDD API) code generation for a table, the Spark counterpart of
  * [[AbstractWayangTableOperatorManager]]. Every abstract operator becomes one RDD transformation,
  * so Spark executes exactly the generated plan (join order included).
  *
  * Rows are Scala tuples produced by the job's parser object ([[ParserCode]]); tuple fields are
  * addressed 1-based (`_1`, `_2`, ...), as in the `fields` maps.
  */
trait AbstractSparkTableOperatorManager extends AbstractTableOperatorManager {

  // ---- building blocks (also used by dataset-specific managers) -----------------------------

  def sparkSourceCode(outVarName: String, file: String, parser: String, name: String): String =
    s"""
       |val $outVarName = sc.textFile(datapath + "$file")
       |  .setName("Read $name")
       |  .map($parser)
       |  .setName("Parse $name to tuple")
    """.stripMargin

  def sparkKeyedReduceCode(inVarName: String, outVarName: String, keyUdf: String): String =
    s"""
       |val $outVarName = $inVarName.keyBy($keyUdf).reduceByKey((t1, t2) => t1).values
      """.stripMargin

  /** Reduces everything to one element without leaving the RDD world (RDD.reduce is an action). */
  def sparkGlobalReduceCode(inVarName: String, outVarName: String): String =
    s"""
       |val $outVarName = $inVarName.map(x => ((), x)).reduceByKey((x1, x2) => x1, 1).values
      """.stripMargin

  def sparkSortCode(inVarName: String, outVarName: String, index: Int): String =
    s"""
       |val $outVarName = $inVarName.sortBy(_._$index)
      """.stripMargin

  // ---- operators ----------------------------------------------------------------------------

  override def dataSourceCode(outVarName: String): String =
    sparkSourceCode(outVarName, s"$tableName.tbl", s"${ParserCode.Token}.$tableName", tableName)

  override def buildDataSourceCode(schema: String, tableName: String, outVarName: String, delimiter: String = "|"): String =
    dataSourceCode(outVarName)

  override def buildFilterCode(inVarName: String, outVarName: String, filterField: String, filterValue: String, filterOp: String = "<="): String = {
    // dates are parsed into the same Int encoding as Wayang's CsvUtils.parseDate
    val value = filterValue.replace("CsvUtils.parseDate(", s"${ParserCode.Token}.parseDate(")
    s"""
       |val $outVarName = $inVarName.filter(x => x._$filterField $filterOp $value)
      """.stripMargin
  }

  override def buildJoinCode(lxVarName: String, rxVarName: String, outVarName: String, joinRelation: JoinRelation): String = {
    val lxJFieldId = joinRelation.lx.fields(joinRelation.lxField).toInt
    val rxJFieldId = joinRelation.rx.fields(joinRelation.field).toInt
    // keep the right-hand tuple, as the Wayang jobs do
    s"""
       |val $outVarName = $lxVarName.keyBy(_._$lxJFieldId).join($rxVarName.keyBy(_._$rxJFieldId)).map(_._2._2)
       """.stripMargin
  }

  override def joinCode(lxVarName: String, rxVarName: String, outVarName: String, joinRelation: JoinRelation): String =
    buildJoinCode(lxVarName, rxVarName, outVarName, joinRelation)

  override def mapCode(inVarName: String, outVarName: String, seed: Int = 0): (String, Int) = {
    val complexity = seed % 3
    val c = complexity match {
      case 0 => s"""
                   |val $outVarName = $inVarName.map(x => x)
                """.stripMargin
      case 1 => s"""
                   |val $outVarName = $inVarName.map(x => {var count = 0; for (v <- x.productIterator) count += 1; x})
                """.stripMargin
      case 2 => s"""
                   |val $outVarName = $inVarName.map(x => {var count = 0; for (v1 <- x.productIterator; v2 <- x.productIterator) count += 1; x})
                """.stripMargin
    }
    (c, complexity)
  }

  override def groupByCode1(inVarName: String, outVarName: String, seed: Int): (String, Double) = {
    val gField = getElementBySeed(groupFields.keys.toSeq, seed).toString
    (sparkKeyedReduceCode(inVarName, outVarName, s"_._${fields(gField)}"), groupFields(gField).toDouble)
  }

  override def groupByCode2(inVarName: String, outVarName1: String, outVarName2: String, seed: Int): (String, Double) = {
    // two consecutive group-bys collapse into one keyed reduce
    val gField = getElementBySeed(groupFields.keys.toSeq, seed).toString
    (sparkKeyedReduceCode(inVarName, outVarName2, s"_._${fields(gField)}"), groupFields(gField).toDouble)
  }

  override def reduceCode1(inVarName: String, outVarName: String): String =
    sparkGlobalReduceCode(inVarName, outVarName)

  /** A Reduce pair (combiner + reduce of a grouped reduce in the input workload) on a group field. */
  override def reduceCode2(inVarName: String, outVarName1: String, outVarName2: String, seed: Int): String = {
    val rField = getElementBySeed(groupFields.keys.toSeq, seed).toString
    sparkKeyedReduceCode(inVarName, outVarName2, s"_._${fields(rField)}")
  }

  override def sortPartitionCode(inVarName: String, outVarName: String, seed: Int): String = {
    val sField = getElementBySeed(fields.keys.toSeq, seed).toString
    sparkSortCode(inVarName, outVarName, fields(sField).toInt)
  }

  override def partitionCode(inVarName: String, outVarName: String): String =
    s"""
       |val $outVarName = $inVarName.keyBy(_._1).partitionBy(new org.apache.spark.HashPartitioner($inVarName.getNumPartitions)).values
      """.stripMargin

  override def sinkCode(inVarName: String, outVarName: String): String =
    s"""
       | return $inVarName
      """.stripMargin
}
