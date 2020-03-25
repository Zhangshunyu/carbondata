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

package org.apache.carbondata.examples.util

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.examples.ConfigUtil

object CarbonSessionUtil {
  private val LOGGER = LogServiceFactory.getLogService(classOf[ConfigUtil].getCanonicalName)

  def createCarbonSession (appName: String, carbonPropertiesFileFullPath: String): SparkSession = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd HH:mm:ss")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE, "true")
      .addProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC, "")
    // load carbon config
    ConfigUtil.loadCarbonConfig(carbonPropertiesFileFullPath, new Configuration())

    val sparkObsConfigFileName = CarbonProperties.getInstance().getProperty("spark.obs.conf.path")
    val defaultCarbonStoreLocation = "s3a://obs-david/warehouse"
    val masterUrl = CarbonProperties.getInstance().getProperty("spark.master.url")

    val realEnvCarbonStoreLocation = if (CarbonProperties.getStorePath != null) {
      LOGGER.info("get the path from carbon properties.")
      CarbonProperties.getStorePath} else {
      LOGGER.info("get the path from default path.")
      defaultCarbonStoreLocation}
    LOGGER.info("real store path is: " + realEnvCarbonStoreLocation)

    import org.apache.spark.sql.CarbonSession._
    val sparkSessionBuilder = SparkSession
      .builder()
      .master(masterUrl)
      .appName(appName)
      .config("spark.sql.crossJoin.enabled", "true")
      .config("spark.sql.warehouse.dir", realEnvCarbonStoreLocation + "dir")
    // load the obs related config into session
    ConfigUtil.loadSparkConfig(sparkObsConfigFileName, new Configuration(), sparkSessionBuilder)

    CarbonProperties.getInstance().addProperty("carbon.storelocation", realEnvCarbonStoreLocation)
    val spark = sparkSessionBuilder
      .enableHiveSupport()
      .getOrCreateCarbonSession(realEnvCarbonStoreLocation, null)
    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

}
