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

import scala.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationStart, SparkListenerTaskEnd, SparkListenerTaskStart}

import java.io.{FileWriter, File}

class ListenerForSpark(conf: SparkConf) extends SparkListener with Logging {

  val recordElapsedTime = 50
  var currentDuration = 0
  var runningTaskNum = 0
  var appId = ""
  var appName = ""
  val sparkHome: String = Properties.envOrElse("SPARK_HOME","/home/root" )
  val reportPath: String = s"$sparkHome/dynamicParallelism"
  val reports = new File(reportPath)
  reports.mkdirs()

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    logInfo("Coordinator: Applicaiton is staertd")
    val timeTag = System.currentTimeMillis()
    appId = applicationStart.appId.getOrElse(s"nil_$timeTag")
    appName = applicationStart.appName
    val p = new Printer()
    p.start()
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    runningTaskNum += 1
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    runningTaskNum -= 1
  }

  class Printer extends Thread {
    override def run(): Unit = {
      while (true) {
        Thread.sleep(recordElapsedTime)
        val taskParallelismFile = new FileWriter(
          s"$reportPath/taskParallelism_$appId.csv",true)
        currentDuration += recordElapsedTime
        taskParallelismFile.write(s"$currentDuration,$runningTaskNum\n")
        logInfo(s"$appName have $runningTaskNum running tasks")
        taskParallelismFile.close()
      }
    }
  }
}
