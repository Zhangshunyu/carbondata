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

package org.apache.spark.carbondata.bucketing

import org.apache.spark.sql.{CarbonEnv, Row}
import org.apache.spark.sql.common.util.Spark2QueryTest
import org.apache.spark.sql.execution.exchange.Exchange
import org.scalatest.BeforeAndAfterAll
import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonProperties

class TableBucketingTestCase extends Spark2QueryTest with BeforeAndAfterAll {

  var threshold: Int = _

  override def beforeAll {

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    threshold = sqlContext.getConf("spark.sql.autoBroadcastJoinThreshold").toInt
    sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold", "-1")
    sql("DROP TABLE IF EXISTS t4")
    sql("DROP TABLE IF EXISTS t5")
    sql("DROP TABLE IF EXISTS t6")
    sql("DROP TABLE IF EXISTS t6_")
    sql("DROP TABLE IF EXISTS t7")
    sql("DROP TABLE IF EXISTS t8")
    sql("DROP TABLE IF EXISTS t9")
    sql("DROP TABLE IF EXISTS t10")
    sql("DROP TABLE IF EXISTS t11")
    sql("DROP TABLE IF EXISTS t12")
    sql("DROP TABLE IF EXISTS t13")
    sql("DROP TABLE IF EXISTS t14")
    sql("DROP TABLE IF EXISTS t15")
  }

  test("test create table with buckets using table properties and loaded data will" +
    " store into different files") {
    sql("CREATE TABLE t4 (ID Int, date Timestamp, country String, name String, phonetype String," +
        "serialname String, salary Int) STORED AS carbondata TBLPROPERTIES " +
        "('BUCKETNUMBER'='4', 'BUCKETCOLUMNS'='name')")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t4")
    val table = CarbonEnv.getCarbonTable(Option("default"), "t4")(sqlContext.sparkSession)
    val segmentDir = FileFactory.getCarbonFile(table.getTablePath + "/Fact/Part0/Segment_0")
    val dataFiles = segmentDir.listFiles(new CarbonFileFilter {
      override def accept(file: CarbonFile): Boolean = file.getName.endsWith(".carbondata")
    })
    assert(dataFiles.length == 4)
    checkAnswer(sql("select count(*) from t4"), Row(100))
    checkAnswer(sql("select count(*) from t4 where name='aaa99'"), Row(1))
    if (table != null && table.getBucketingInfo() != null) {
      assert(true)
    } else {
      assert(false, "Bucketing info does not exist")
    }
  }

  test("test create carbon table with buckets like hive sql") {
    sql("CREATE TABLE t13 (ID Int, date Timestamp, country String, name String, phonetype String," +
      "serialname String, salary Int) STORED AS carbondata CLUSTERED BY (name) INTO 4 BUCKETS")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t13")
    val table = CarbonEnv.getCarbonTable(Option("default"), "t13")(sqlContext.sparkSession)
    val segmentDir = FileFactory.getCarbonFile(table.getTablePath + "/Fact/Part0/Segment_0")
    val dataFiles = segmentDir.listFiles(new CarbonFileFilter {
      override def accept(file: CarbonFile): Boolean = file.getName.endsWith(".carbondata")
    })
    assert(dataFiles.length == 4)
    checkAnswer(sql("select count(*) from t13"), Row(100))
    checkAnswer(sql("select count(*) from t13 where name='aaa99'"), Row(1))
    if (table != null && table.getBucketingInfo() != null) {
      assert(true)
    } else {
      assert(false, "Bucketing info does not exist")
    }
  }

  test("test create table with buckets unsafe") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT, "true")
    sql("CREATE TABLE t10 (ID Int, date Timestamp, country String, name String, phonetype String," +
        "serialname String, salary Int) STORED AS carbondata TBLPROPERTIES " +
        "('BUCKETNUMBER'='4', 'BUCKETCOLUMNS'='name')")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t10")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT, "false")
    val table: CarbonTable = CarbonMetadata.getInstance().getCarbonTable("default", "t10")
    if (table != null && table.getBucketingInfo() != null) {
      assert(true)
    } else {
      assert(false, "Bucketing info does not exist")
    }
  }

  test("must be unable to create if number of buckets is in negative number") {
    try {
      sql(
        """
           CREATE TABLE t9
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           USING carbondata
           OPTIONS("bucketnumber"="-1", "bucketcolumns"="name")
        """)
      assert(false)
    }
    catch {
      case malformedCarbonCommandException: MalformedCarbonCommandException => assert(true)
    }
  }

  test("must unable to create table if number of buckets is 0") {
    try{
      sql(
        """
          |CREATE TABLE t11
          |(ID Int,
          | date Timestamp,
          | country String,
          | name String,
          | phonetype String,
          | serialname String,
          | salary Int)
          | STORED AS carbondata
          | TBLPROPERTIES('bucketnumber'='0', 'bucketcolumns'='name')
        """.stripMargin
      )
      assert(false)
    }
    catch {
      case malformedCarbonCommandException: MalformedCarbonCommandException => assert(true)
    }
  }

  test("test create table with both no bucket join of carbon tables") {
    sql("CREATE TABLE t5 (ID Int, date Timestamp, country String, name String, phonetype String," +
        "serialname String, salary Int) STORED AS carbondata")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t5")
    val plan = sql(
      """
        |select t1.*, t2.*
        |from t5 t1, t5 t2
        |where t1.name = t2.name
      """.stripMargin).queryExecution.executedPlan
    checkAnswer(sql(
      """select count(*) from
        |(select t1.*, t2.*
        |from t5 t1, t5 t2
        |where t1.name = t2.name) temp
      """.stripMargin), Row(100))
    var shuffleExists = false
    plan.collect {
      case s: Exchange if (s.getClass.getName.equals
      ("org.apache.spark.sql.execution.exchange.ShuffleExchange") ||
        s.getClass.getName.equals
        ("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec"))
      => shuffleExists = true
    }
    assert(shuffleExists, "shuffle should exist on non bucket tables")
  }

  test("test join of carbon bucket table and non bucket parquet table") {
    sql("CREATE TABLE t8 (ID Int, date Timestamp, country String, name String, phonetype String," +
      "serialname String, salary Int) STORED AS carbondata TBLPROPERTIES " +
      "('BUCKETNUMBER'='4', 'BUCKETCOLUMNS'='name')")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t8")

    sql("DROP TABLE IF EXISTS parquet_table")
    sql("select * from t8").write
      .format("parquet")
      .saveAsTable("parquet_table")

    val plan = sql(
      """
        |select t1.*, t2.*
        |from t8 t1, parquet_table t2
        |where t1.name = t2.name
      """.stripMargin).queryExecution.executedPlan
    checkAnswer(sql(
      """select count(*) from
        |(select t1.*, t2.*
        |from t8 t1, parquet_table t2
        |where t1.name = t2.name) temp
      """.stripMargin), Row(100))
    var shuffleExists = false
    plan.collect {
      case s: Exchange if (s.getClass.getName.equals
      ("org.apache.spark.sql.execution.exchange.ShuffleExchange") ||
        s.getClass.getName.equals
        ("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec"))
      => shuffleExists = true
    }
    assert(shuffleExists, "shuffle should exist on non bucket tables")
    sql("DROP TABLE parquet_table")
  }

  test("test no shuffle when using bucket tables") {
    sql("CREATE TABLE t12 (ID Int, date Timestamp, country String, name String, phonetype String," +
      "serialname String, salary Int) STORED AS carbondata CLUSTERED BY (name) INTO 4 BUCKETS")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t12")

    sql("DROP TABLE IF EXISTS bucketed_parquet_table")
    sql("select * from t12").write
      .format("parquet")
      .bucketBy(4, "name")
      .saveAsTable("bucketed_parquet_table")

    checkAnswer(sql("select count(*) from t12"), Row(100))
    checkAnswer(sql("select count(*) from bucketed_parquet_table"), Row(100))

    val plan = sql(
      """
        |select t1.*, t2.*
        |from t12 t1, bucketed_parquet_table t2
        |where t1.name = t2.name
      """.stripMargin).queryExecution.executedPlan
    var shuffleExists = false
    plan.collect {
      case s: Exchange if (s.getClass.getName.equals
      ("org.apache.spark.sql.execution.exchange.ShuffleExchange") ||
        s.getClass.getName.equals
        ("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec"))
      => shuffleExists = true
    }
    assert(!shuffleExists, "shuffle should not exist on bucket tables")
    sql("DROP TABLE bucketed_parquet_table")
  }

  test("test join of carbon bucket tables") {
    sql("CREATE TABLE t6 (ID Int, date Timestamp, country String, name String, phonetype String," +
        "serialname String, salary Int) STORED AS carbondata TBLPROPERTIES " +
        "('BUCKETNUMBER'='4', 'BUCKETCOLUMNS'='name')")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t6")
    sql("CREATE TABLE t6_ (ID Int, date Timestamp, country String, name String, phonetype String," +
      "serialname String, salary Int) STORED AS carbondata TBLPROPERTIES " +
      "('BUCKETNUMBER'='4', 'BUCKETCOLUMNS'='name')")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t6_")
    val plan = sql(
      """
        |select t1.*, t2.*
        |from t6 t1, t6_ t2
        |where t1.name = t2.name
      """.stripMargin).queryExecution.executedPlan
    checkAnswer(sql(
      """select count(*) from
        |(select t1.*, t2.*
        |from t6 t1, t6_ t2
        |where t1.name = t2.name) temp
      """.stripMargin), Row(100))
    var shuffleExists = false
    plan.collect {
      case s: Exchange if (s.getClass.getName.equals
      ("org.apache.spark.sql.execution.exchange.ShuffleExchange") ||
        s.getClass.getName.equals
        ("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec"))
      => shuffleExists = true
    }
    assert(!shuffleExists, "shuffle should not exist on bucket tables")
  }

  test("test join of carbon bucket table and parquet bucket table") {
    sql("CREATE TABLE t7 (ID Int, date Timestamp, country String, name String, phonetype String," +
        "serialname String, salary Int) STORED AS carbondata TBLPROPERTIES " +
        "('BUCKETNUMBER'='4', 'BUCKETCOLUMNS'='name')")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t7")

    sql("DROP TABLE IF EXISTS bucketed_parquet_table")
    sql("select * from t7").write
      .format("parquet")
      .bucketBy(4, "name")
      .saveAsTable("bucketed_parquet_table")
    // carbon join parquet, both bucket tables.
    val plan = sql(
      """
        |select t1.*, t2.*
        |from t7 t1, bucketed_parquet_table t2
        |where t1.name = t2.name
      """.stripMargin).queryExecution.executedPlan
    // parquet join parquet, both bucket tables.
    checkAnswer(sql(
      """select count(*) from
        |(select t1.*, t2.*
        |from t7 t1, bucketed_parquet_table t2
        |where t1.name = t2.name) temp
      """.stripMargin), Row(100))
    var shuffleExists = false
    plan.collect {
      case s: Exchange if (s.getClass.getName.equals
      ("org.apache.spark.sql.execution.exchange.ShuffleExchange") ||
        s.getClass.getName.equals
        ("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec"))
      => shuffleExists = true
    }
    assert(!shuffleExists, "shuffle should not exist on bucket tables")
    sql("DROP TABLE bucketed_parquet_table")
  }

  test("test join of carbon bucket tables using hive sql") {
    sql("CREATE TABLE t14 (ID Int, date Timestamp, country String, name String, phonetype String," +
      "serialname String, salary Int) STORED AS carbondata CLUSTERED BY (name) INTO 4 BUCKETS")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t14")
    sql("CREATE TABLE t15 (ID Int, date Timestamp, country String, name String, phonetype String," +
      "serialname String, salary Int) STORED AS carbondata CLUSTERED BY (name) INTO 4 BUCKETS")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE t15")
    val plan = sql(
      """
        |select t1.*, t2.*
        |from t14 t1, t15 t2
        |where t1.name = t2.name
      """.stripMargin).queryExecution.executedPlan
    checkAnswer(sql(
      """select count(*) from
        |(select t1.*, t2.*
        |from t14 t1, t15 t2
        |where t1.name = t2.name) temp
      """.stripMargin), Row(100))
    var shuffleExists = false
    plan.collect {
      case s: Exchange if (s.getClass.getName.equals
      ("org.apache.spark.sql.execution.exchange.ShuffleExchange") ||
        s.getClass.getName.equals
        ("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec"))
      => shuffleExists = true
    }
    assert(!shuffleExists, "shuffle should not exist on bucket tables")
  }

  override def afterAll {
    sql("DROP TABLE IF EXISTS t4")
    sql("DROP TABLE IF EXISTS t5")
    sql("DROP TABLE IF EXISTS t6")
    sql("DROP TABLE IF EXISTS t6_")
    sql("DROP TABLE IF EXISTS t7")
    sql("DROP TABLE IF EXISTS t8")
    sql("DROP TABLE IF EXISTS t9")
    sql("DROP TABLE IF EXISTS t10")
    sql("DROP TABLE IF EXISTS t11")
    sql("DROP TABLE IF EXISTS t12")
    sql("DROP TABLE IF EXISTS t13")
    sql("DROP TABLE IF EXISTS t14")
    sql("DROP TABLE IF EXISTS t15")
    sql("DROP TABLE IF EXISTS bucketed_parquet_table")
    sql("DROP TABLE IF EXISTS parquet_table")
    sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold", threshold.toString)
  }
}
