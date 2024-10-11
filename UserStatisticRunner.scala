import org.apache.spark.sql.SparkSession
import global.jobs.user_statistic.UserStatistic

object UserStatisticRunner {
  def main(args: Array[String]): Unit = {

    val configs : Map[String, String] = Map(
      "table_name" -> "table_relation",
      "keyspace" -> "user_data_kp"
    )

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("ProcessUserStatistic")
      .config("spark.cassandra.connection.host", "localhost")
      .config("spark.cassandra.connection.port", "9042")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    UserStatistic.processData(configs, spark)
  }
}
