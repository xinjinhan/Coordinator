/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.shuhai.coordinator

import org.apache.spark.{SparkConf, Success}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._

import java.io.{File, FileWriter}
import scala.collection.mutable
import scala.io.{BufferedSource, Source}
import scala.util.Properties

class CoordinatorListenerForSpark(conf: SparkConf) extends SparkListener with Logging {

  val sampleElapsedTime = 500
  var currentSampleTime = 0
  var runningTaskNum = 0
  var ratioOfTasksToTotalCPUCores = 0.00
  var fullParallelismRunningTime = 0
  var parallelismAndTime = 0
  var startTime: Long = 0
  var currentExecutorIdToTotalCores: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()
  var appId = ""
  var appName = ""
  val sparkHome: String = Properties.envOrElse("SPARK_HOME","/home/root" )
  val reportPath: String = s"$sparkHome/coordinatorReport"
  val reports = new File(reportPath)
  reports.mkdirs()
  val reportFile = new FileWriter(
    s"$reportPath/coordinator.report",true)
  val reportFileSource: BufferedSource = Source.fromFile(s"$reportPath/coordinator.report")

  if (reportFileSource.getLines().isEmpty) {
    reportFile.write("ApplicationName, duration, AverageParallelism, FullParallelismRunningTime, RatioOfFullParallelismRunningTime, FinalStatus\n")
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    startTime = applicationStart.time
    logInfo("Coordinator listener is started")
    val timeTag = System.currentTimeMillis()
    appId = applicationStart.appId.getOrElse(s"$timeTag")
    appName = applicationStart.appName
    val listener = new Listener()
    listener.start()
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    conf.set("spark.executor.memory",s"${stageSubmitted.stageInfo.stageId.asInstanceOf[String]}g")
    conf.set("spark.executor.cores",s"${stageSubmitted.stageInfo.stageId.asInstanceOf[String]}")
    logWarning(s"cores is ${conf.get("spark.executor.cores")}")
    logWarning(s"memory is ${conf.get("spark.executor.memory")}")
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    runningTaskNum += 1
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    runningTaskNum -= 1
  }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    currentExecutorIdToTotalCores.put(executorAdded.executorId, executorAdded.executorInfo.totalCores)
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    currentExecutorIdToTotalCores.remove(executorRemoved.executorId)
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    val stopTime = applicationEnd.time
    val duration = stopTime - startTime
    reportFile.write(s"$appName, ${duration / 1000.0}, ${(parallelismAndTime.asInstanceOf[Double] /
      duration.asInstanceOf[Double]).formatted("%.2f")}, ${fullParallelismRunningTime / 1000.0}," +
      s" ${(fullParallelismRunningTime.asInstanceOf[Double] / duration.asInstanceOf[Double]).formatted("%.2f")}\n")
    reportFile.close()
    reportFileSource.close()
  }

  class Listener extends Thread {
    override def run(): Unit = {
      while (true) {
        Thread.sleep(sampleElapsedTime)
        var currentTotalCores = 0
        currentExecutorIdToTotalCores.foreach(c => currentTotalCores += c._2)
        currentSampleTime += sampleElapsedTime
        parallelismAndTime += runningTaskNum * sampleElapsedTime
        ratioOfTasksToTotalCPUCores = runningTaskNum.asInstanceOf[Double]/currentTotalCores.asInstanceOf[Double]
      if (ratioOfTasksToTotalCPUCores == 1) {
        fullParallelismRunningTime += sampleElapsedTime
      }
      logInfo(s"$appName have $runningTaskNum running tasks")
      }
    }
  }

}
