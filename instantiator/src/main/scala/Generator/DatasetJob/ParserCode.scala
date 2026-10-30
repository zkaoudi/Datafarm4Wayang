package Generator.DatasetJob

/**
  * Generated jobs that parse their input themselves (JOB on Wayang/Spark, TPC-H on Spark) put the
  * parse functions into an object `<jobName>Parsers` in the same file. Operator code refers to it
  * through this placeholder, which the job template replaces with the actual object name.
  */
object ParserCode {
  val Token = "__PARSERS__"
}
