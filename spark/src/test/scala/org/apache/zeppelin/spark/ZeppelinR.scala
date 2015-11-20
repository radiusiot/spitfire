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

package org.apache.zeppelin.spark

object ZeppelinR {
/*
  private val R = RClient()

  def open(master: String = "local[*]", sparkHome: String): Unit = {
    R.eval(
      s"""
         |Sys.setenv(SPARK_HOME="$sparkHome")
         |.libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"), .libPaths()))
         |library(SparkR)
         |sc <- sparkR.init(master="$master")
         |sqlContext <- sparkRSQL.init(jsc = sc)
       """.
        stripMargin
    )
    eval("library('knitr')")
    eval(
      """
        |getFunctionNames <- function() {
        |   loaded <- (.packages())
        |   loaded <- paste("package:", loaded, sep ="")
        |   return(sort(unlist(lapply(loaded, lsf.str))))
        | }
      """.stripMargin
    )
  }

  def eval(command: String): Any = {
    R.eval(command)
  }

  def set(key: String, value: AnyRef): Unit = {
    R.set(key, value)
  }

  def get(key: String): Any = {
    R.get(key)._1
  }

  def close():Unit = {
    R.eval("""
         |sparkR.stop()
       """.stripMargin
    )
  }
*/
}
