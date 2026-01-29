package Generator.DatasetJob.tpc_h.table

import Generator.DatasetJob.{AbstractDatasetOperatorManager, AbstractTableOperatorManager,AbstractWayangTableOperatorManager}

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
