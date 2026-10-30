package Generator

/**
  * Source template of a generated Wayang job.
  *
  * Generated jobs only use the public Apache Wayang API (wayang-api-scala-java, wayang-java,
  * wayang-spark), so they compile against Apache Wayang main (Scala 2.12, Java 17) without any
  * other module. Each job is one self-contained `.scala` file holding
  *
  *  - `object <jobName>` with
  *     - `buildPlan(planBuilder, datapath)`: builds the job's Wayang plan and returns its final
  *       `DataQuanta`, without executing anything;
  *     - `main(args)`: `<platforms> <datapath>`, registers the requested platforms
  *       (`java`, `spark` or `java,spark`), executes the plan by collecting its output and
  *       prints the number of output records and the runtime;
  *  - optionally an object with the dataset's CSV parsers (TPC-H, JOB).
  *
  * `datapath` is the folder with the input files; a plain local path is turned into a `file:` URL
  * (Wayang's local file system only accepts `file:` URLs) and always ends with `/`.
  */
object WayangJobTemplate {

  /** Package of the generated jobs. */
  val Package = "org.apache.wayang.datafarm.generated"

  private val Template: String =
    """/*
      | * Licensed to the Apache Software Foundation (ASF) under one
      | * or more contributor license agreements.  See the NOTICE file
      | * distributed with this work for additional information
      | * regarding copyright ownership.  The ASF licenses this file
      | * to you under the Apache License, Version 2.0 (the
      | * "License"); you may not use this file except in compliance
      | * with the License.  You may obtain a copy of the License at
      | *
      | *     http://www.apache.org/licenses/LICENSE-2.0
      | *
      | * Unless required by applicable law or agreed to in writing, software
      | * distributed under the License is distributed on an "AS IS" BASIS,
      | * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
      | * See the License for the specific language governing permissions and
      | * limitations under the License.
      | */
      |
      |package //#package#//
      |
      |import org.apache.wayang.api._
      |import org.apache.wayang.core.api.{Configuration, WayangContext}
      |import org.apache.wayang.java.Java
      |import org.apache.wayang.spark.Spark
      |
      |/** //#description#// */
      |object //#job_name#// {
      |
      |  /** Builds the plan of this job and returns its final DataQuanta; nothing is executed. */
      |  def buildPlan(planBuilder: PlanBuilder, datapath: String): DataQuanta[_] = {
      |
      |//#body#//
      |
      |  }
      |
      |  /** Usage: //#job_name#// <platforms: java | spark | java,spark> <datapath> */
      |  def main(args: Array[String]): Unit = {
      |    if (args.length < 2) {
      |      System.err.println("Usage: //#job_name#// <platforms: java | spark | java,spark> <datapath>")
      |      sys.exit(1)
      |    }
      |    val wayangCtx = new WayangContext(new Configuration)
      |    args(0).split(",").map(_.trim.toLowerCase).filter(_.nonEmpty).foreach {
      |      case "java" => wayangCtx.register(Java.basicPlugin())
      |      case "spark" => wayangCtx.register(Spark.basicPlugin())
      |      case p => throw new IllegalArgumentException(s"Unknown platform '$p' (expected java and/or spark)")
      |    }
      |    val planBuilder = new PlanBuilder(wayangCtx).withJobName("//#title#// (//#job_name#//)")
      |
      |    val start = System.currentTimeMillis()
      |    val output = buildPlan(planBuilder, toUrl(args(1))).collect()
      |    println(s"//#job_name#//: ${output.size} output records in ${System.currentTimeMillis() - start} ms")
      |  }
      |
      |  /** Turns a local path into a file: URL ending with '/'; URLs with a scheme are kept. */
      |  def toUrl(path: String): String = {
      |    val url = if (path.contains(":/")) path else new java.io.File(path).getAbsoluteFile.toURI.toString
      |    if (url.endsWith("/")) url else url + "/"
      |  }
      |}
      |//#parsers#//
      |""".stripMargin

  /**
    * @param jobName     name of the job object (and of the file)
    * @param title       dataset name used in the Wayang job name, e.g. "TPC-H"
    * @param description one-line description in the object's doc comment
    * @param body        generated operator code; may refer to `planBuilder` and `datapath`
    *                    and must end with `return <final DataQuanta>`
    * @param parsers     extra top-level definitions appended to the file (e.g. CSV parsers)
    */
  def render(jobName: String, title: String, description: String, body: String, parsers: String = ""): String =
    Template
      .replace("//#package#//", Package)
      .replace("//#description#//", description)
      .replace("//#title#//", title)
      .replace("//#body#//", indent(body, "    "))
      .replace("//#parsers#//", if (parsers.trim.isEmpty) "" else "\n" + parsers + "\n")
      .replace("//#job_name#//", jobName)

  private def indent(code: String, prefix: String): String =
    code.split("\n", -1).map(l => if (l.trim.isEmpty) "" else prefix + l.trim).mkString("\n")
}
