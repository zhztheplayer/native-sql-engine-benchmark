package org.apache.spark.nsebench

import java.util.Locale

import org.apache.spark.SparkContext
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.execution.benchmark.{SqlBasedBenchmark, TPCDSQueryBenchmarkArguments}
import org.apache.spark.sql.execution.datasources.LogicalRelation

import scala.collection.mutable.ListBuffer


class TPCDSBenchmarkBase extends SqlBasedBenchmark {

  override def getSparkSession: SparkSession = {
    SparkSession.builder.getOrCreate()
  }

  val tables = Seq("catalog_page", "catalog_returns", "customer", "customer_address",
    "customer_demographics", "date_dim", "household_demographics", "inventory", "item",
    "promotion", "store", "store_returns", "catalog_sales", "web_sales", "store_sales",
    "web_returns", "web_site", "reason", "call_center", "warehouse", "ship_mode", "income_band",
    "time_dim", "web_page")

  def setupTables(dataLocation: String, format: String, enablePerRowStats: Boolean): Map[String, Long] = {
    tables.map { tableName =>
      spark.read.format(format).load(s"$dataLocation/$tableName").createOrReplaceTempView(tableName)
      tableName -> (if (enablePerRowStats) {
        spark.table(tableName).count()
      } else {
        0L
      })
    }.toMap
  }

  def runTpcdsQueries(
      queryLocation: String,
      queries: Seq[String],
      tableSizes: Map[String, Long],
      nameSuffix: String = ""): Unit = {
    queries.foreach { name =>
      val queryString = resourceToString(s"$queryLocation/$name.sql",
        classLoader = Thread.currentThread().getContextClassLoader)

      // This is an indirect hack to estimate the size of each query's input by traversing the
      // logical plan and adding up the sizes of all tables that appear in the plan.
      val queryRelations = scala.collection.mutable.HashSet[String]()
      spark.sql(queryString).queryExecution.analyzed.foreach {
        case SubqueryAlias(alias, _: LogicalRelation) =>
          queryRelations.add(alias.name)
        case LogicalRelation(_, _, Some(catalogTable), _) =>
          queryRelations.add(catalogTable.identifier.table)
        case HiveTableRelation(tableMeta, _, _, _, _) =>
          queryRelations.add(tableMeta.identifier.table)
        case _ =>
      }
      val numRows = queryRelations.map(tableSizes.getOrElse(_, 0L)).sum
      val benchmark = new Benchmark(s"TPCDS", numRows, 2, output = output)
      val caseName = s"$name$nameSuffix"
      benchmark.addCase(caseName) { i =>
        spark.sparkContext.setJobDescription("TPCDS %s ITERATION %d".format(name, i))
        spark.sql(queryString).noop()
      }
      try {
        benchmark.run()
      } catch {
        case e: Throwable =>
          e.printStackTrace()
          println(s"Case %s failed to run (%s), skipping... ".format(caseName, e.getMessage))
      }
    }
  }

  def filterQueries(
      origQueries: Seq[String],
      args: TPCDSQueryBenchmarkArguments): Seq[String] = {
    if (args.queryFilter.nonEmpty) {
      origQueries.filter(args.queryFilter.contains)
    } else {
      origQueries
    }
  }

  private def optionMatch(optionName: String, s: String): Boolean = {
    optionName == s.toLowerCase(Locale.ROOT)
  }

  var ENABLE_V2_7 = false
  var USE_PARQUET_FORMAT = false
  var ENABLE_PER_ROW_STATS = false

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    var argList = mainArgs.toList
    val tmpBenchArgs: ListBuffer[String] = ListBuffer()

    while (argList.nonEmpty) {
      argList match {
        case optName :: value :: tail if optionMatch("--data-location", optName) =>
          argList = tail
          tmpBenchArgs.append(optName, value)
        case optName :: value :: tail if optionMatch("--query-filter", optName) =>
          argList = tail
          tmpBenchArgs.append(optName, value)
        case optName :: tail if optionMatch("--enable-v2.7", optName) =>
          argList = tail
          ENABLE_V2_7 = true
        case optName :: tail if optionMatch("--use-parquet-format", optName) =>
          argList = tail
          USE_PARQUET_FORMAT = true
        case optName :: tail if optionMatch("--enable-per-row-stats", optName) =>
          argList = tail
          ENABLE_PER_ROW_STATS = true
        case _ =>
          throw new IllegalArgumentException("Unrecognizable options: " + argList)
      }
    }

    val benchmarkArgs = new TPCDSQueryBenchmarkArguments(tmpBenchArgs.toArray)

    // List of all TPC-DS v1.4 queries
    val tpcdsQueries = Seq(
      "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10", "q11",
      "q12", "q13", "q14a", "q14b", "q15", "q16", "q17", "q18", "q19", "q20",
      "q21", "q22", "q23a", "q23b", "q24a", "q24b", "q25", "q26", "q27", "q28", "q29", "q30",
      "q31", "q32", "q33", "q34", "q35", "q36", "q37", "q38", "q39a", "q39b", "q40",
      "q41", "q42", "q43", "q44", "q45", "q46", "q47", "q48", "q49", "q50",
      "q51", "q52", "q53", "q54", "q55", "q56", "q57", "q58", "q59", "q60",
      "q61", "q62", "q63", "q64", "q65", "q66", "q67", "q68", "q69", "q70",
      "q71", "q72", "q73", "q74", "q75", "q76", "q77", "q78", "q79", "q80",
      "q81", "q82", "q83", "q84", "q85", "q86", "q87", "q88", "q89", "q90",
      "q91", "q92", "q93", "q94", "q95", "q96", "q97", "q98", "q99")

    // This list only includes TPC-DS v2.7 queries that are different from v1.4 ones
    val tpcdsQueriesV2_7 = Seq(
      "q5a", "q6", "q10a", "q11", "q12", "q14", "q14a", "q18a",
      "q20", "q22", "q22a", "q24", "q27a", "q34", "q35", "q35a", "q36a", "q47", "q49",
      "q51a", "q57", "q64", "q67a", "q70a", "q72", "q74", "q75", "q77a", "q78",
      "q80a", "q86a", "q98")

    // If `--query-filter` defined, filters the queries that this option selects
    val queriesV1_4ToRun = filterQueries(tpcdsQueries, benchmarkArgs)
    val queriesV2_7ToRun = filterQueries(tpcdsQueriesV2_7, benchmarkArgs)

    if ((queriesV1_4ToRun ++ queriesV2_7ToRun).isEmpty) {
      throw new RuntimeException(
        s"Empty queries to run. Bad query name filter: ${benchmarkArgs.queryFilter}")
    }

    val format = if(USE_PARQUET_FORMAT) "parquet" else "arrow"
    val tableSizes = setupTables(benchmarkArgs.dataLocation, format, ENABLE_PER_ROW_STATS)
    runTpcdsQueries(queryLocation = "tpcds-double", queries = queriesV1_4ToRun, tableSizes)
    if (ENABLE_V2_7) {
      runTpcdsQueries(queryLocation = "tpcds-v2.7.0-double", queries = queriesV2_7ToRun, tableSizes,
        nameSuffix = "-v2.7")
    }
  }
}
