name := "SparkTest"

version := "1.0"

scalaVersion := "2.10.2"
libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "2.2.0"
libraryDependencies +=  "com.databricks" % "spark-xml_2.10" %"0.4.1"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "2.2.0"