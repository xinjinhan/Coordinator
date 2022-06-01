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

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._

import java.io.{File, FileWriter}
import scala.collection.mutable
import scala.io.Source
import scala.util.Properties

class CoordinatorListenerForSpark(conf: SparkConf) extends SparkListener with Logging {

  val sampleElapsedTime = 5
  var currentSampleTime = 0
  var runningTaskNum = 0
  var currentExecutorIdToTotalCores: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()
  var appId = ""
  var appName = ""
  val sparkHome: String = Properties.envOrElse("SPARK_HOME","/home/root" )
  val reportPath: String = s"$sparkHome/coordinatorReport"
  val reports = new File(reportPath)
  reports.mkdirs()

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    logInfo("Coordinator listener is started")
    val timeTag = System.currentTimeMillis()
    appId = applicationStart.appId.getOrElse(s"$timeTag")
    appName = applicationStart.appName
    val listener = new Listener()
    listener.run()
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

  class Listener extends Thread {
    override def run(): Unit = {
      while (true) {
        Thread.sleep(sampleElapsedTime)
        var currentTotalCores = 0
        currentExecutorIdToTotalCores.foreach(c => currentTotalCores += c._2)
        val reportFile = new FileWriter(
          s"$reportPath/${appName}_$appId.csv",true)
        val reportFileSource = Source.fromFile(s"$reportPath/${appName}_$appId.csv")
        if (reportFileSource.getLines().isEmpty) {
          reportFile.write("ApplicationName,SampleTime,RunningTaskNumber,TotalAvailableCPUCores,RatioOfTasksToTotalCPUCores\n")
        }
        currentSampleTime += sampleElapsedTime
        reportFile.write(s"" +
          s"$appName," +
          s"$currentSampleTime," +
          s"$runningTaskNum," +
          s"$currentTotalCores," +
          s"${(runningTaskNum.asInstanceOf[Double]/currentTotalCores.asInstanceOf[Double]).formatted("%.2f")}" +
          s"\n")
        logInfo(s"$appName have $runningTaskNum running tasks")
        reportFile.close()
        reportFileSource.close()
      }
    }
  }
}
