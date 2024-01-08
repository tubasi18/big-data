ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "indexingaStreamofTweetsintoMongoDBusingSparkStreaming"
  )
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.1.2"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.1.2"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.1.2"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.1.2"
libraryDependencies += "org.mongodb.spark" %% "mongo-spark-connector" % "3.0.1"
libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "2.9.0"
libraryDependencies += "org.mongodb" % "mongo-java-driver" % "3.12.10"

libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-20" % "7.14.0"