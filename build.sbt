ThisBuild / name := "Scala Spark"
ThisBuild / version := "1.0"
ThisBuild / scalaVersion := "2.11.2"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.3"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.3" % "provided"

resolvers += Resolver.mavenLocal