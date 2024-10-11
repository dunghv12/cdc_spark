package global.jobs.user_statistic

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession, functions => F}
import com.datastax.spark.connector._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.SimpleStatement
import global.domain.Relation
import org.apache.spark.sql.cassandra.{DataFrameReaderWrapper, DataFrameWriterWrapper}
import org.apache.spark.sql.functions.col


object UserStatistic {
  def processData(configs: Map[String, String], spark: SparkSession): Unit = {
    import spark.implicits._

    val tableName = configs("table_name")
    val keySpace = configs("keyspace")
    val statTableName = configs.get("stat_table_name").getOrElse("statistic")

    val cdcDf = spark.read
      .cassandraFormat(
        table = s"${tableName}_scylla_cdc_log",
        keyspace = keySpace,
        pushdownEnable = false)
      .load()

    val relationDf = spark.read
      .cassandraFormat(
        table = tableName,
        keyspace = keySpace,
        pushdownEnable = false)
      .load()

    val cdcColRenamedDf = cdcDf.columns.foldLeft(cdcDf) {
      case (tempDf, colName) => tempDf.withColumnRenamed(colName, colName.replace("$", "_"))
    }

    if (tableExists(keySpace, statTableName)) {
      val statHistoryDf = spark.read
        .cassandraFormat(
          table = statTableName,
          keyspace = keySpace,
          pushdownEnable = false)
        .load()
        .repartition(10)


      val cdcCountDf = cdcColRenamedDf
        .filter(F.col("cdc_operation") === 2)
        .groupBy(Relation.FROM_INDIV, Relation.RELATION_TYPE)
        .count()

      val cdcPivotDf = cdcCountDf
        .groupBy(Relation.FROM_INDIV)
        .pivot(Relation.RELATION_TYPE)
        .sum("count")
        .repartition(10)


      val unionDf = cdcPivotDf
        .unionByName(statHistoryDf, allowMissingColumns = true)

      val sumColumns = unionDf.columns.map(colName => F.sum(F.col(colName)).alias(colName))
      val resultDf =
        unionDf
        .groupBy(Relation.FROM_INDIV)
        .agg(sumColumns.head, sumColumns.tail:_*)

      val totalDf =
        resultDf
          .withColumn("total",
            resultDf.columns.filter(_ != "from_indiv").map(c => F.coalesce(col(c), F.lit(0))).reduce(_ + _))

      totalDf.orderBy(Relation.FROM_INDIV).show(1000, truncate = false)

    }
    else {
      val countDf = relationDf
        .groupBy(F.col(Relation.FROM_INDIV), F.col(Relation.RELATION_TYPE))
        .count()

      val pivotDf = countDf
        .groupBy(Relation.FROM_INDIV)
        .pivot(Relation.RELATION_TYPE)
        .sum("count")

      createTableFromDataFrame(pivotDf.schema, keySpace, statTableName)

      pivotDf
        .write
        .cassandraFormat(table = statTableName, keyspace = keySpace)
        .mode(SaveMode.Append)
        .save()

    }
  }

  def createTableFromDataFrame(schema: org.apache.spark.sql.types.StructType, keyspace: String, tableName: String): Unit = {

    val session = CqlSession.builder()
      .withKeyspace(keyspace)
      .build()

    val columns = schema.fields.map(field => {
      val columnName = field.name
      val dataType = field.dataType match {
        case org.apache.spark.sql.types.IntegerType => "INT"
        case org.apache.spark.sql.types.StringType => "UUID"
        case org.apache.spark.sql.types.LongType => "BIGINT"
        case org.apache.spark.sql.types.TimestampType => "TIMESTAMP"
        case _ => throw new IllegalArgumentException(s"Unsupported data type: ${field.dataType}")
      }
      s"$columnName $dataType"
    }).mkString(", ")

    val primaryKey = "from_indiv"

    val createTableCql =
      s"""
         |CREATE TABLE IF NOT EXISTS $keyspace.$tableName (
         |  $columns,
         |  PRIMARY KEY ($primaryKey)
         |);
           """.stripMargin

    session.execute(SimpleStatement.newInstance(createTableCql))
    session.close()
    println(s"Table $keyspace.$tableName created if it did not exist.")
  }

  def tableExists(keyspace: String, tableName: String): Boolean = {
    val session = CqlSession.builder()
      .withKeyspace(keyspace) // Specify the keyspace
      .build()

    val query = s"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '$keyspace' AND table_name = '$tableName'"

    val resultSet = session.execute(SimpleStatement.newInstance(query))

    val exists = resultSet.iterator().hasNext

    session.close()

    exists
  }
}
