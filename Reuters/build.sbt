ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"
//ThisBuild / scalaVersion := "2.11.8"

lazy val root = (project in file("."))
  .settings(
    name := "Reuters"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.1",
  "org.apache.spark" %% "spark-sql" % "3.5.1" )

//config for the SoftNet Cluster
//libraryDependencies ++= Seq(
//  "org.apache.spark" %% "spark-core" % "2.3.1",
//  "org.apache.spark" %% "spark-sql" % "2.3.1",
//  "org.apache.hadoop" % "hadoop-client" % "3.3.1")
