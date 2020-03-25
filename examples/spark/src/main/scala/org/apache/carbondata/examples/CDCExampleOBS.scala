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

package org.apache.carbondata.examples

import java.time.LocalDateTime

import scala.collection.JavaConverters._
import scala.util.Random

import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.examples.util.CarbonSessionUtil

/**
 * CDC (Change Data Capture) example, it reads input CSV files as Change input file and merge
 * it into a target CarbonData table
 *
 * The schema of target table:
 * (id int, value string, remark string, mdt timestamp)
 *
 * The schema of change table:
 * (id int, value string, change_type string, mdt timestamp)
 *
 * change_type can be I/U/D
 *
 * This example generate N number batch of change data and merge them into target table
 */
// scalastyle:off
object CDCExampleOBS {

  case class Target (id: Int, value: String, remark: String, mdt: String)
  // 1 int and 44 string columns
  case class GridTarget (id: Int, app_code: String, ym: String, cons_id: String, org_num: String,
    veri_rslt: String, veri_remark: String,cacl_date: String, send_date: String, cons_no: String,
    cons_name: String, cons_sort_code: String, elec_addr: String,trade_type_code: String,
    elec_type_code: String, volt_code: String, hec_trade_code: String,
    create_date: String, power_on_date: String, cancel_date: String, status_code: String,
    busi_region_no: String, transfer_flag: String, mr_sect_num: String, gp_code: String,
    contract_cap: String, shift_num: String, rural_cons_code: String, mic_ym: String
    , market_prop_sort: String, extend_filed_time_stamp: String, extend_field_src_system: String,
    extend_field_valid_flag: String, extend_field_update_flag: String,
    extend_field_update_time: String, extend_field_1: String, extend_field_2: String,
    extend_field_3: String, extend_field_4: String, extend_field_5: String,
    extend_field_6: String, source: String, change_type: String, op_ts: String,
    current_ts: String)

  case class Change (id: Int, value: String, change_type: String, mdt: String)
  // 1 int and 44 string columns
  case class GridChange (id: Int, app_code: String, ym: String, cons_id: String, org_num: String,
    veri_rslt: String, veri_remark: String,cacl_date: String, send_date: String, cons_no: String,
    cons_name: String, cons_sort_code: String, elec_addr: String,trade_type_code: String,
    elec_type_code: String, volt_code: String, hec_trade_code: String,
    create_date: String, power_on_date: String, cancel_date: String, status_code: String,
    busi_region_no: String, transfer_flag: String, mr_sect_num: String, gp_code: String,
    contract_cap: String, shift_num: String, rural_cons_code: String, mic_ym: String
    , market_prop_sort: String, extend_filed_time_stamp: String, extend_field_src_system: String,
    extend_field_valid_flag: String, extend_field_update_flag: String,
    extend_field_update_time: String, extend_field_1: String, extend_field_2: String,
    extend_field_3: String, extend_field_4: String, extend_field_5: String,
    extend_field_6: String, source: String, change_type: String, op_ts: String,
    current_ts: String)


  // User can set it to "carbon" or "hive"
  // If "carbon" is set, will use CarbonData's MERGE to perform CDC
  // If "hive" is set, will use INSERT OVERWRITE to perform CDC
  private var solution = "carbon"

  // print result or not to console for debugging
  private val printDetail = true

  // number of records for target table before start CDC
  private var numInitialRows = 10000

  // number of records to insert for every batch
  private var numInsertPerBatch = 100

  // number of records to update for every batch
  private var numUpdatePerBatch = 900

  // number of records to delete for every batch
  private var numDeletePerBatch = 100

  // number of batch to simulate CDC
  private var numBatch = 10

  private val random = new Random()

  // generate 1000 random strings
  private val values =
    (1 to 1000).map { x =>
      // to simulate a wide target table, make a relatively long string for value
      random.nextString(16)
    }

  // pick one value randomly
  private def pickValue = values(random.nextInt(values.size))

  // IDs in the target table
  private val currentIds = new java.util.ArrayList[Int](numInitialRows * 2)
  private def getId(index: Int) = currentIds.get(index)
  private def getAndRemoveId(index: Int) = currentIds.remove(index)
  private def addId(id: Int) = currentIds.add(id)
  private def removeId(index: Int) = currentIds.remove(index)
  private def numOfIds = currentIds.size
  private def maxId: Int = currentIds.asScala.max

  private val INSERT = "I"
  private val UPDATE = "U"
  private val DELETE = "D"

  // generate change data for insert
  private def generateRowsForInsert(sparkSession: SparkSession) = {
    // data for insert to the target table
    val insertRows = (maxId + 1 to maxId + numInsertPerBatch).map { x =>
      addId(x)
      GridChange(x,
        pickValue, pickValue, pickValue, pickValue, pickValue, pickValue, pickValue,
        pickValue, pickValue, pickValue, pickValue, pickValue, pickValue, pickValue,
        pickValue, pickValue, pickValue, pickValue, pickValue, pickValue, pickValue,
        pickValue, pickValue, pickValue, pickValue, pickValue, pickValue, pickValue,
        pickValue, pickValue, pickValue, pickValue, pickValue, pickValue, pickValue,
        pickValue, pickValue, pickValue, pickValue, pickValue, pickValue,
        INSERT, LocalDateTime.now().toString, LocalDateTime.now().toString)
    }
    sparkSession.createDataFrame(insertRows)
  }

  // generate change data for delete
  private def generateRowsForDelete(sparkSession: SparkSession) = {
    val deletedRows = (1 to numDeletePerBatch).map { x =>
      val idIndex = random.nextInt(numOfIds)
      GridChange(getAndRemoveId(idIndex),
        pickValue, pickValue, pickValue, pickValue, pickValue, pickValue, pickValue,
        pickValue, pickValue, pickValue, pickValue, pickValue, pickValue, pickValue,
        pickValue, pickValue, pickValue, pickValue, pickValue, pickValue, pickValue,
        pickValue, pickValue, pickValue, pickValue, pickValue, pickValue, pickValue,
        pickValue, pickValue, pickValue, pickValue, pickValue, pickValue, pickValue,
        pickValue, pickValue, pickValue, pickValue, pickValue, pickValue,
        DELETE, LocalDateTime.now().toString, LocalDateTime.now().toString)
    }
    sparkSession.createDataFrame(deletedRows)
  }

  // generate change data for update
  private def generateRowsForUpdate(sparkSession: SparkSession) = {
    val updatedRows = (1 to numUpdatePerBatch).map { x =>
      val idIndex = random.nextInt(numOfIds)
      GridChange(getId(idIndex),
        pickValue, pickValue, pickValue, pickValue, pickValue, pickValue, pickValue,
        pickValue, pickValue, pickValue, pickValue, pickValue, pickValue, pickValue,
        pickValue, pickValue, pickValue, pickValue, pickValue, pickValue, pickValue,
        pickValue, pickValue, pickValue, pickValue, pickValue, pickValue, pickValue,
        pickValue, pickValue, pickValue, pickValue, pickValue, pickValue, pickValue,
        pickValue, pickValue, pickValue, pickValue, pickValue, pickValue,
        UPDATE, LocalDateTime.now().toString, LocalDateTime.now().toString)
    }
    sparkSession.createDataFrame(updatedRows)
  }

  // generate initial data for target table
  private def generateTarget(sparkSession: SparkSession): Unit = {
    print("================= generating target table...")
    val time = timeIt { () =>
      val insertRows = (1 to numInitialRows).map { x =>
        addId(x)
        GridTarget(x,
          pickValue, pickValue, pickValue, pickValue, pickValue, pickValue, pickValue,
          pickValue, pickValue, pickValue, pickValue, pickValue, pickValue, pickValue,
          pickValue, pickValue, pickValue, pickValue, pickValue, pickValue, pickValue,
          pickValue, pickValue, pickValue, pickValue, pickValue, pickValue, pickValue,
          pickValue, pickValue, pickValue, pickValue, pickValue, pickValue, pickValue,
          pickValue, pickValue, pickValue, pickValue, pickValue, pickValue,
          "origin", LocalDateTime.now().toString, LocalDateTime.now().toString)
      }
      println(insertRows.size)
      // here we insert duplicated rows to simulate non primary key table (which has repeated id)
      val duplicatedRow = insertRows.union(insertRows)
      val targetData = sparkSession.createDataFrame(duplicatedRow)
      println("start to writer init target table")
      targetData.repartition(8)
        .write
        .format("carbondata")
        .option("tableName", "target_table_carbon")
        .mode(SaveMode.Overwrite)
        .save()
    }
    println(s"================= generated target table ${timeFormatted(time)}")
  }

  // generate change data
  private def generateChange(sparkSession: SparkSession): Unit = {
    val update = generateRowsForUpdate(sparkSession)
    val delete = generateRowsForDelete(sparkSession)
    val insert = generateRowsForInsert(sparkSession)

    // union them so that the change contains IUD
    update
      .union(delete)
      .union(insert)
      .repartition(8)
      .write
      .format("carbondata")
      .option("tableName", "change_table_carbon")
      .mode(SaveMode.Overwrite)
      .save()
  }

  private def readTargetData(sparkSession: SparkSession): Dataset[Row] =
    sparkSession.read
      .format("carbondata")
      .option("tableName", "target_table_carbon")
      .load()

  private def readChangeData(sparkSession: SparkSession): Dataset[Row] =
    sparkSession.read
      .format("carbondata")
      .option("tableName", "change_table_carbon")
      .load()

  private def timeIt(func: () => Unit): Long = {
    val start = System.nanoTime()
    func()
    System.nanoTime() - start
  }

  private def timeFormatted(updateTime: Long) = {
    (updateTime.asInstanceOf[Double] / 1000 / 1000 / 1000).formatted("%.2f") + " s"
  }

  private def printTarget(spark: SparkSession, i: Int) = {
    if (printDetail) {
      println(s"================================== target table after CDC batch$i")
      spark.sql("select count(*) from target_table_carbon ").show(false)
    }
  }

  private def printChange(spark: SparkSession, i: Int) = {
    if (printDetail) {
      println(s"-- CDC batch$i")
      spark.sql("select count(*) from change_table_carbon").show()
    }
  }

  private def createSession(carbonProPath: String) = {
    import org.apache.spark.sql.CarbonSession._
    val spark = CarbonSessionUtil.createCarbonSession("CDCExampleOBS", carbonProPath)
    spark
  }

  def main(args: Array[String]): Unit = {
    // file_path solution numInitialRows numInsertPerBatch numUpdatePerBatch numDeletePerBatch numBatch
    assert(args.length > 0)
    CarbonProperties.setAuditEnabled(false);
    val carbonProPath = if (args(0).nonEmpty) args(0) else throw new Exception("carbon properties" +
      " file path is needed!")
    if (args.length == 7) {
      solution = args(1)
      numInitialRows = args(2).toInt
      numInsertPerBatch = args(3).toInt
      numUpdatePerBatch = args(4).toInt
      numDeletePerBatch = args(5).toInt
      numBatch = args(6).toInt
      println(s"solution: $solution, numInitialRows: $numInitialRows," +
        s" numInsertPerBatch: $numInsertPerBatch, numUpdatePerBatch: $numUpdatePerBatch," +
        s" numDeletePerBatch: $numDeletePerBatch, numBatch: $numBatch")
    }
    val spark = createSession(carbonProPath)

    println(s"-- start CDC example using $solution solution")
    spark.sql("drop table if exists target_table_carbon")
    spark.sql("drop table if exists change_table_carbon")

    // prepare target table
    generateTarget(spark)

    if (printDetail) {
      println("## -- target table")
      println("## ============ init target table")
      spark.sql("select count(*) from target_table_carbon").show()
    }

    var updateTime = 0L

    // Do CDC for N batch
    (1 to numBatch).foreach { i =>
      // prepare for change data
      generateChange(spark)

      printChange(spark, i)

      // apply change to target table
      val time = timeIt { () =>
        print(s"applying change batch$i...")
        if (solution.equals("carbon")) {
          carbonSolution(spark)
        } else {
          carbonInsertOverwriteSolution(spark)
        }
      }
      updateTime += time
      println(s"====================================== done! ${timeFormatted(time)}")
      printTarget(spark, i)
    }

    // do a query after all changes to compare query time
    val queryTime = timeIt {
      () => spark.sql("select * from target_table_carbon").collect()
    }

    // print update time
    println("=====================================================")
    println(s"-- total update takes ${timeFormatted(updateTime)}")

    // print query time
    println(s"--total query takes ${timeFormatted(queryTime)}")
    println("=====================================================")

    spark.close()
  }

  /**
   * Solution leveraging carbon's Merge syntax to apply change data
   */
  private def carbonSolution(spark: SparkSession) = {
    import org.apache.spark.sql.CarbonSession._

    // find the latest value for each key
    val latestChangeForEachKey = readChangeData(spark)
      .selectExpr("id", "struct(app_code , ym , cons_id , org_num ,veri_rslt , veri_remark ,cacl_date , send_date , cons_no , cons_name , cons_sort_code , elec_addr ,trade_type_code ,elec_type_code , volt_code , hec_trade_code ,create_date , power_on_date , cancel_date , status_code ,busi_region_no , transfer_flag , mr_sect_num , gp_code , contract_cap , shift_num , rural_cons_code , mic_ym, market_prop_sort , extend_filed_time_stamp , extend_field_src_system ,extend_field_valid_flag , extend_field_update_flag ,extend_field_update_time , extend_field_1 , extend_field_2 ,extend_field_3 , extend_field_4 , extend_field_5 , extend_field_6 , source , change_type , op_ts ,current_ts ) as otherCols" )
      .groupBy("id")
      .agg(max("otherCols").as("latest"))
      .selectExpr("id", "latest.*")

    val target = readTargetData(spark)
    target.as("A")
      .merge(latestChangeForEachKey.as("B"), "A.id = B.id")
      .whenMatched("B.change_type = 'D'")
      .delete()
      .whenMatched("B.change_type = 'U'")
      .updateExpr(
        Map("id" -> "B.id", "app_code" -> "B.app_code", "ym" -> "B.ym","cons_id" -> "B.cons_id", "org_num" -> "B.org_num", "veri_rslt" -> "B.veri_rslt","veri_remark" -> "B.veri_remark", "cacl_date" -> "B.cacl_date", "send_date" -> "B.send_date","cons_no" -> "B.cons_no", "cons_name" -> "B.cons_name", "cons_sort_code" -> "B.cons_sort_code","elec_addr" -> "B.elec_addr", "trade_type_code" -> "B.trade_type_code","elec_type_code" -> "B.elec_type_code","volt_code" -> "B.volt_code","hec_trade_code" -> "B.hec_trade_code","create_date" -> "B.create_date","power_on_date" -> "B.power_on_date","cancel_date" -> "B.cancel_date","status_code" -> "B.status_code","busi_region_no" -> "B.busi_region_no","transfer_flag" -> "B.transfer_flag","mr_sect_num" -> "B.mr_sect_num","gp_code" -> "B.gp_code","contract_cap" -> "B.contract_cap","shift_num" -> "B.shift_num","rural_cons_code" -> "B.rural_cons_code","mic_ym" -> "B.mic_ym","market_prop_sort" -> "B.market_prop_sort","extend_filed_time_stamp" -> "B.extend_filed_time_stamp","extend_field_src_system" -> "B.extend_field_src_system","extend_field_valid_flag" -> "B.extend_field_valid_flag","extend_field_update_flag" -> "B.extend_field_update_flag","extend_field_update_time" -> "B.extend_field_update_time","extend_field_1" -> "B.extend_field_1","extend_field_2" -> "B.extend_field_2","extend_field_3" -> "B.extend_field_3","extend_field_4" -> "B.extend_field_4","extend_field_5" -> "B.extend_field_5","extend_field_6" -> "B.extend_field_6","source" -> "B.source","change_type" -> "B.change_type","op_ts" -> "B.op_ts","current_ts" -> "B.current_ts"))
      .whenNotMatched("B.change_type = 'I'")
      .insertExpr(
        Map("id" -> "B.id", "app_code" -> "B.app_code", "ym" -> "B.ym","cons_id" -> "B.cons_id", "org_num" -> "B.org_num", "veri_rslt" -> "B.veri_rslt","veri_remark" -> "B.veri_remark", "cacl_date" -> "B.cacl_date", "send_date" -> "B.send_date","cons_no" -> "B.cons_no", "cons_name" -> "B.cons_name", "cons_sort_code" -> "B.cons_sort_code","elec_addr" -> "B.elec_addr", "trade_type_code" -> "B.trade_type_code","elec_type_code" -> "B.elec_type_code","volt_code" -> "B.volt_code","hec_trade_code" -> "B.hec_trade_code","create_date" -> "B.create_date","power_on_date" -> "B.power_on_date","cancel_date" -> "B.cancel_date","status_code" -> "B.status_code","busi_region_no" -> "B.busi_region_no","transfer_flag" -> "B.transfer_flag","mr_sect_num" -> "B.mr_sect_num","gp_code" -> "B.gp_code","contract_cap" -> "B.contract_cap","shift_num" -> "B.shift_num","rural_cons_code" -> "B.rural_cons_code","mic_ym" -> "B.mic_ym","market_prop_sort" -> "B.market_prop_sort","extend_filed_time_stamp" -> "B.extend_filed_time_stamp","extend_field_src_system" -> "B.extend_field_src_system","extend_field_valid_flag" -> "B.extend_field_valid_flag","extend_field_update_flag" -> "B.extend_field_update_flag","extend_field_update_time" -> "B.extend_field_update_time","extend_field_1" -> "B.extend_field_1","extend_field_2" -> "B.extend_field_2","extend_field_3" -> "B.extend_field_3","extend_field_4" -> "B.extend_field_4","extend_field_5" -> "B.extend_field_5","extend_field_6" -> "B.extend_field_6","source" -> "B.source","change_type" -> "B.change_type","op_ts" -> "B.op_ts","current_ts" -> "B.current_ts"))
      .execute()
  }

  /**
   * Typical solution when using hive
   * INSERT OVERWRITE to rewrite the whole table/partition for every CDC batch
   */
  private def carbonInsertOverwriteSolution(spark: SparkSession) = {
    val latestChangeForEachKey = readChangeData(spark)
      .selectExpr("id", "struct(app_code , ym , cons_id , org_num ,veri_rslt , veri_remark ,cacl_date , send_date , cons_no , cons_name , cons_sort_code , elec_addr ,trade_type_code ,elec_type_code , volt_code , hec_trade_code ,create_date , power_on_date , cancel_date , status_code ,busi_region_no , transfer_flag , mr_sect_num , gp_code , contract_cap , shift_num , rural_cons_code , mic_ym, market_prop_sort , extend_filed_time_stamp , extend_field_src_system ,extend_field_valid_flag , extend_field_update_flag ,extend_field_update_time , extend_field_1 , extend_field_2 ,extend_field_3 , extend_field_4 , extend_field_5 , extend_field_6 , source , change_type , op_ts ,current_ts ) as otherCols" )
      .groupBy("id")
      .agg(max("otherCols").as("latest"))
      .selectExpr("id", "latest.*")
    latestChangeForEachKey.createOrReplaceTempView("latest_change")
    spark.sql(
      """
        | insert overwrite table target_table_carbon
        | select * from
        | (
        |   select B.*
        |     from target_table_carbon A
        |     right join latest_change B
        |     on A.id = B.id
        |     where B.change_type = 'U'
        |   union all
        |     select B.*
        |     from latest_change B
        |     where B.change_type = 'I'
        |   union all
        |     select A.*
        |     from target_table_carbon A
        |     left join latest_change B
        |     on A.id = B.id
        |     where B.id is null
        | ) T
      """.stripMargin)
  }
}
// scalastyle:on
