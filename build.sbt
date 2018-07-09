organization := "com.snowplow"
name := "take-home"
version := "1.0-SNAPSHOT"
scalaVersion := "2.11.8"

lazy val scalaBinVersion = "2.11"
lazy val slf4jVersion = "1.7.25"
lazy val pureconfigVersion = "0.9.0"
lazy val sparkVersion = "2.3.0"
lazy val scalatestVersion = "3.0.5"
lazy val sparkTestingBaseVersion = "2.3.0_0.9.0"

libraryDependencies ++= Seq(
  // Config reading
  "com.github.pureconfig" %% "pureconfig" % pureconfigVersion,
  // Logging
  "org.slf4j" % "slf4j-api" % slf4jVersion,
  "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
  // Spark SQL
  "org.apache.spark" % ("spark-sql_" + scalaBinVersion) % sparkVersion % Provided,
  // Testing
  "org.scalatest" % ("scalatest_" + scalaBinVersion) % scalatestVersion % Test,
  "com.holdenkarau" %% "spark-testing-base" % sparkTestingBaseVersion % Test,
  "org.apache.spark" %% "spark-hive" % sparkVersion % Test // required for spark-testing-base
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "native", xs @ _*)    => MergeStrategy.first
  case PathList("META-INF", "services", xs @ _*)  => MergeStrategy.filterDistinctLines
  case PathList("META-INF", xs @ _*)              => MergeStrategy.discard
  case _                                          => MergeStrategy.first
}
