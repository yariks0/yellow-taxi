ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.19"

lazy val root = (project in file("."))
  .settings(
    name := "yellow-taxi",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-hive" % sparkVersion % Provided,
      "org.apache.iceberg" %% s"iceberg-spark-runtime-$sparkMajorVersion" % icebergVersion % Provided,
      "org.apache.hadoop" % "hadoop-aws" % "3.3.4",
      "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.262",
    )
  )

val sparkVersion = "3.5.1"
val sparkMajorVersion = "3.5"
val icebergVersion = "1.4.3"
val hadoopAWSVersion = "3.3.4"
val awsSDKVersion = "1.12.262"
