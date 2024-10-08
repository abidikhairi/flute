ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.12"
val sparkVersion = "2.4.5"

lazy val root = (project in file("."))
  .settings(
    name := "flute",
    idePackagePrefix := Some("com.majesteye.rnd.pipelines")
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
