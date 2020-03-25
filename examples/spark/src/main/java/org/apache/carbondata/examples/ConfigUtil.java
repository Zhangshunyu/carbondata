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

package org.apache.carbondata.examples;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.util.CarbonProperties;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

public class ConfigUtil {
  private static Logger LOGGER =
          LogServiceFactory.getLogService(ConfigUtil.class.getCanonicalName());

  /**
   * load carbon.properties into CarbonProperties
   */
  public static void loadCarbonConfig(final String configFile, final Configuration hadoopConf) throws IOException {
    if (!configFile.isEmpty()) {
      LOGGER.info("====" + configFile);
      Properties properties = loadProperties(configFile, hadoopConf);
      // add all property into CarbonProperties
      Iterator<String> iterator = properties.stringPropertyNames().iterator();
      CarbonProperties carbonProperties = CarbonProperties.getInstance();
      while (iterator.hasNext()) {
        String key = iterator.next();
        String value = properties.getProperty(key).trim();
        LOGGER.info(key + "=" + value);
        carbonProperties.addProperty(key, value);
      }
    } else {
      LOGGER.info("no carbon property file config, no need to load.");
    }
  }

  /**
   * load spark-obs.conf to overwrite the default config of Spark session
   */
  public static void loadSparkConfig(String configFileName, final Configuration hadoopConf,
                                     final SparkSession.Builder builder) throws Exception {
    Properties properties = new Properties();
    String configFilePath = configFileName;

    File configFile = new File(ConfigUtil.class.
            getResource("/").getPath() + "/" + configFileName);
    if (configFile.exists()) {
      configFilePath = new File(ConfigUtil.class.
              getResource("/").getPath() + "/" + configFileName).getCanonicalPath();
      properties = loadProperties(configFilePath, hadoopConf);
    } else {
      try (FileInputStream fileInputStream = new FileInputStream(configFileName)) {
        properties.load(fileInputStream);
      } catch (Exception e) {
        LOGGER.error("Failed to load spark conf ", e);
        throw e;
      }
    }
    // add all property into SparkConf
    Iterator<String> iterator = properties.stringPropertyNames().iterator();
    LOGGER.info("start to load config file " + configFilePath);
    while (iterator.hasNext()) {
      String key = iterator.next();
      String value = properties.getProperty(key).trim();
      builder.config(key, value);
      if (!key.endsWith(".key")) {
        // ak sk should not print out into log!
        LOGGER.info(key + "=" + value);
      }
    }
  }

  /**
   * load a property file in obs into Properties object
   */
  private static Properties loadProperties(final String configFile,
                                           final Configuration hadoopConf) {
    Properties properties = new Properties();
    DataInputStream dataInputStream = null;
    try {
      CarbonFile file = FileFactory.getCarbonFile(configFile, hadoopConf);
      dataInputStream = file.getDataInputStream(4096);
      properties.load(dataInputStream);
    } catch (IOException e) {
      LOGGER.error("Failed to load config file " + configFile, e);
    } finally {
      if (dataInputStream != null) {
        try {
          dataInputStream.close();
        } catch (IOException e) {
          LOGGER.error("Failed to close config file " + configFile, e);
        }
      }
    }
    return properties;
  }
}

