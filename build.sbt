
ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"


libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "2.4.0"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.2" ,
  "org.apache.spark" %% "spark-sql" % "3.1.2" ,
  "com.datastax.spark" %% "spark-cassandra-connector" % "3.1.0",
  "joda-time" % "joda-time" % "2.10.10"
)

libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "3.5.1" // Adjust version as needed


assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.first
}
resolvers += "Spark Packages Repo" at "https://repos.spark-packages.org/"
dependencyOverrides += "org.scala-lang.modules" %% "scala-parser-combinators" % "2.4.0"
//libraryDependencySchemes += "org.scala-lang.modules" %% "scala-parser-combinators" % VersionScheme.EarlySemVer
lazy val root = (project in file("."))
  .settings(
    name := "StatisticScyllaDB",
    assembly / assemblyJarName := "statistic_scylladb.jar"
  )
