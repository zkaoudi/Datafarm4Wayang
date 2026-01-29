package Generator

import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox
import scala.reflect.runtime.universe._
import java.io.File
import java.nio.file.Paths
/**
  * Created by researchuser7 on 2020-03-17.
  */

case class AbstractWayangJob(jobBody:String, jobName:String, saveExecutionPlan:Boolean=false) extends AbstractJob {

  override def fillBody(): String ={

    AbstractWayangJob.JOB_TEMPLATE
      .replace("//#job_name#//", this.jobName)
      .replace("//#body#//", this.jobBody)
  }

  override def fillSBT(): String = {
    AbstractWayangJob.SBT_TEMPLATE
      .replace("//#job_name#//", this.jobName)
  }

  override def createProject(projectPath:String): Unit ={

    val finalProjectPath = Paths.get(projectPath, jobName).toString
    println(s"Saving Wayang job files project to $finalProjectPath")

    val jobWriter = new java.io.PrintWriter(s"$projectPath/$jobName.scala")
    try {
      jobWriter.write(filledBody)
    }
    finally {
      jobWriter.close()
    }
  }
}

object AbstractWayangJob {

  val SBT_TEMPLATE: String =
    """
       |name := "//#job_name#//"
       |
       |version := "0.1"
       |
       |scalaVersion := "2.11.12"
       |
       |val flinkVersion = "1.10.0"
       |val flinkConf = "provided" // "compile" | "provided"
       |
       |val flinkDependencies = Seq(
  irintln("body: " + jobBody)
       |  "org.apache.flink" %% "flink-scala" % flinkVersion % flinkConf,
       |  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % flinkConf,
       |  "org.apache.flink" % "flink-hadoop-fs" % flinkVersion % "compile"
       |)
       |
       |val utilsDependencies = Seq(
       |  "joda-time" % "joda-time" % "2.8.1"
       |)
       |
       |libraryDependencies ++= flinkDependencies
       |libraryDependencies ++= utilsDependencies
       |
       |assemblyJarName in assembly := "//#job_name#//.jar"
     """.stripMargin

  val JOB_TEMPLATE: String =
    """
  | /*
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
  | package org.apache.wayang.ml.generatables
  |
  | import org.apache.wayang.api._
  | import org.apache.wayang.core.api.{Configuration, WayangContext}
  | import org.apache.wayang.core.plugin.Plugin
  | import org.apache.wayang.apps.tpch.data.{Customer, Order, LineItem, Nation, Part, PartSupplier, Supplier} 
  | import org.apache.wayang.core.plugin.Plugin
  | import java.sql.Date
  | import org.apache.wayang.apps.util.{ExperimentDescriptor, Parameters, ProfileDBHelper}
  | import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
  | import org.apache.wayang.apps.tpch.CsvUtils
  | import org.apache.wayang.ml.training.GeneratableJob
  | import org.apache.wayang.core.function.UDFComplexity
  |
  | class //#job_name#// extends GeneratableJob {
  |
  |def buildPlan(args: Array[String]): DataQuanta[_] = {
  | println("running Job: //#job_name#//")
  |
  | val dateFormatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")
  | val plugins = Parameters.loadPlugins(args(0))
  | val datapath = args(1) 
  |
  | implicit val configuration = new Configuration
  | val wayangCtx = new WayangContext(configuration)
  | plugins.foreach(wayangCtx.register)
  | val planBuilder = new PlanBuilder(wayangCtx)
  | .withJobName(s"TPC-H (${this.getClass.getSimpleName})")
  |
  |   //#body#//
  |
  | }
  |}  
  """.stripMargin

  def saveExecPlanCode() = s"""saveExecutionPlan(execPlan, execTime, params, env)"""




}
