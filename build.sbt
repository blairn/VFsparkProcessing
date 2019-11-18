name := "scalaprocessing"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.22.0"