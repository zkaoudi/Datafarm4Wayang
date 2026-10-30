package Generator.DatasetJob.tpc_h.table

import Generator.{AbstractJob, AbstractWayangJob}
import Generator.DatasetJob.{AbstractDatasetOperatorManager, AbstractTableOperatorManager,AbstractWayangTableOperatorManager}
import Generator.DatasetJob.tpc_h.TPCHSchema

import scala.collection.mutable
import scala.util.Random

/**
  * Created by researchuser7 on 2020-04-20.
  */


class TPCHDatasetOperatorManager extends AbstractDatasetOperatorManager {

  val tables: Seq[String] = Seq(
    "lineitem",
    "orders",
    "customer",
    "partsupp",
    "part",
    "supplier"//,
    //"nation"
  )
  def this(platform: String) {
    this()
    this.platform = platform
  }
  /** Wayang jobs get their own TPC-H parsers object, so they need no TPC-H classes in Wayang. */
  override def wayangJob(jobBody: String, jobName: String, saveExecutionPlan: Boolean, tables: Seq[String]): AbstractJob = {
    val parsersObject = jobName + "Parsers"
    AbstractWayangJob(
      jobBody.replace(TPCHSchema.ParsersToken, parsersObject), jobName, saveExecutionPlan,
      title = "TPC-H", parsers = TPCHSchema.parsersCode(parsersObject, tables))
  }

  def getTableOperatorManager(s: String, p: String): AbstractTableOperatorManager  = 
  p match {
    case "Wayang" => {
       s match {
          case "lineitem" => new LineitemOperatorManager with AbstractWayangTableOperatorManager 
          case "orders" => new OrdersOperatorManager with AbstractWayangTableOperatorManager
          case "customer" => new CustomerOperatorManager with AbstractWayangTableOperatorManager
          case "partsupp" => new PartsuppOperatorManager with AbstractWayangTableOperatorManager
          case "part" => new PartOperatorManager with AbstractWayangTableOperatorManager
          case "supplier" => new SupplierOperatorManager with AbstractWayangTableOperatorManager
          case "nation" => new NationOperatorManager with AbstractWayangTableOperatorManager
          case _ => throw new Exception(s"Can not find table operator manager for table '$s' ")
       }
    }
    case _ => { 
      s match {
          case "lineitem" => LineitemOperatorManager()
          case "orders" => OrdersOperatorManager()
          case "customer" => CustomerOperatorManager()
          case "partsupp" => PartsuppOperatorManager()
          case "part" => PartOperatorManager()
          case "supplier" => SupplierOperatorManager()
          case "nation" => NationOperatorManager()
          case _ => throw new Exception(s"Can not find table operator manager for table '$s' ")
        }
      }
    }

  def main(args: Array[String]): Unit = {
    for (i <- 0 to 100){
      try{
        val (tableSequence, joinSequence) = getSourceTableSequence(5, i)
        joinSequence.foreach( j => println(s"> $j"))
        println()
        println(s"Table sequence: $tableSequence")
      } catch {
        case ex:Exception => println(ex)
      } finally {
        println("---------------------------------------------")
      }
    }
  }
}
