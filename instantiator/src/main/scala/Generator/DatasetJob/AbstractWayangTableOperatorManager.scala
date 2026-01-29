package Generator.DatasetJob

import Generator.DatasetJob.utils.getElementBySeed


/**
  * Created by researchuser7 on 2020-04-21.
  */
trait AbstractWayangTableOperatorManager extends AbstractTableOperatorManager {

  val tableName: String

  //val tableCardinality: Int

  val typeSchema: String

  val fields: Map[String, String]

  val joinFieldTable: Map[String, Map[String, String]]

  val filterFieldValue: Map[String, Map[String, Seq[String]]]

  val groupFields: Map[String, String]

  //Operator builders

  override def buildDataSourceCode(schema: String, tableName: String, outVarName: String, delimiter:String = "|"): String = {
    s"""
    | val $outVarName = planBuilder
    | .readTextFile(datapath + "$tableName.tbl")
    | .withName("Read $wayangName")
    | .map($wayangName.parseCsv)
    | .map($wayangName.toTuple)
    | .withName("Parse $wayangName to tuple")
    """.stripMargin
  }

  override def buildFilterCode(inVarName: String, outVarName: String, filterField: String, filterValue: String, filterOp: String = "<="): String = {
    s"""
       |val $outVarName = $inVarName.filter(x=>x._$filterField $filterOp $filterValue)
      """.stripMargin
  }

  override def buildReduceCode2(inVarName: String, outVarName1: String, outVarName2: String, groupByField: String): String = {
      s"""
       |val $outVarName2 = $inVarName.reduce((x1, x2) => x1)
      """.stripMargin
  }

  override def buildGroupByCode1(inVarName: String, outVarName: String, groupByField: String): String = {
       val field = if (groupByField == "0") "1" else groupByField

        s"""
       |val $outVarName = $inVarName.reduceByKey(_._$field, (t1,t2) => t1)
          """.stripMargin

         // groupByKey(_._$groupByField).reduceGroup{v => v.reduce((x1,x2) => x1)}
  }

  override def buildGroupByCode2(inVarName: String, outVarName1: String, outVarName2: String, groupByField: String): String = {
    //TODO check aggregation function complexity
   val field = if (groupByField == "0") "1" else groupByField

        s"""
       |val $outVarName2 = $inVarName.reduceByKey(_._$field, (t1,t2) => t1)
          """.stripMargin
  }

  override def buildSortPartitionCode(inVarName: String, outVarName: String, sortField: String, order: String = "ASCENDING"): String = {
    s"""
       |val $outVarName = $inVarName.sort(_._$sortField)
      """.stripMargin
  }

  override def buildJoinCode(lxVarName: String, rxVarName: String, outVarName: String, joinRelation: JoinRelation): String = {
    val lxJFieldId = joinRelation.lx.fields(joinRelation.field).toInt
    val rxJFieldId = joinRelation.rx.fields(joinRelation.field).toInt

    s"""
       |val $outVarName = $lxVarName.keyBy[Long](_._$lxJFieldId).join($rxVarName.keyBy[Long](_._$rxJFieldId)).map(x => x.field1)
       """.stripMargin
  }
  //Operators already implemented for every table

  override def reduceCode1(inVarName: String, outVarName: String): String = {
    s"""
       |val $outVarName = $inVarName.reduce((x1, x2) => x1)
      """.stripMargin
  }


  override def mapCode(inVarName: String, outVarName: String, seed: Int = 0): (String, Int) = {
    val complexity = seed % 3
    val c = complexity match {
      case 0 => s"""
                   |val $outVarName = $inVarName.map(x=>x).withUdfComplexity(UDFComplexity.LINEAR)
                """.stripMargin
      case 1 => s"""
                   |val $outVarName = $inVarName.map(x=> {var count = 0; for (v <- x.productIterator) count+=1; x})
                   |                  .withUdfComplexity(UDFComplexity.QUADRATIC)
                """.stripMargin
      case 2 => s"""
                   |val $outVarName = $inVarName.map(x=> {var count = 0; for (v1 <- x.productIterator; v2 <- x.productIterator) count+=1; x})
                   |                  .withUdfComplexity(UDFComplexity.SUPERQUADRATIC)
       """.stripMargin
    }
    (c, complexity)
  }

  override def joinCode(lxVarName: String, rxVarName: String, outVarName: String, joinRelation: JoinRelation): String = {
    buildJoinCode(lxVarName: String, rxVarName: String, outVarName: String, joinRelation: JoinRelation)
  }

  override def partitionCode(inVarName: String, outVarName: String): String = {
    s"""
      |val $outVarName = $inVarName.mapPartitions(x=>x)
      """.stripMargin

  }

  override def dataSourceCode(outVarName: String): String = buildDataSourceCode(typeSchema, tableName, outVarName)

  override def groupByCode1(inVarName: String, outVarName: String, seed: Int): (String, Double) = {
    val gField = getElementBySeed(groupFields.keys.toSeq, seed).toString
    val gFieldId = fields(gField).toInt-1
    val code = buildGroupByCode1(inVarName, outVarName, gFieldId.toString)
    (code, groupFields(gField).toDouble)
  }

  override def groupByCode2(inVarName: String, outVarName1: String, outVarName2: String, seed: Int): (String, Double) = {
    val gField = getElementBySeed(groupFields.keys.toSeq, seed).toString
    val gFieldId = fields(gField).toInt-1
    val code = buildGroupByCode2(inVarName, outVarName1, outVarName2, gFieldId.toString)
    (code, groupFields(gField).toDouble)
  }

  override def reduceCode2(inVarName: String, outVarName1: String, outVarName2: String, seed: Int): String = {
    val rField = getElementBySeed(groupFields.keys.toSeq, seed).toString
    val rFieldId = fields(rField).toInt-1
    val code = buildReduceCode2(inVarName, outVarName1, outVarName2, rFieldId.toString)
    code
  }

  override def sortPartitionCode(inVarName: String, outVarName: String, seed: Int): String = {
    val sField = getElementBySeed(fields.keys.toSeq, seed).toString
    val sFieldId = fields(sField).toInt
    buildSortPartitionCode(inVarName, outVarName, sFieldId.toString)
  }

  override def sinkCode(inVarName: String, outVarName: String): String = {
    s"""
      | return $inVarName.collectNoExecute()
      """.stripMargin
  }

}
