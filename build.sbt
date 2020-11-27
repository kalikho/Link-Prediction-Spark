name := """OsintProject"""

version := "0.1"

scalaVersion := "2.11.12"


// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.5"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5"

// https://mvnrepository.com/artifact/org.apache.spark/spark-graphx
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.4.5"

resolvers += "SparkPackages" at "https://dl.bintray.com/spark-packages/maven"

// https://mvnrepository.com/artifact/graphframes/graphframes
//libraryDependencies += "graphframes" % "graphframes" % "0.7.0-spark2.4-s_2.11"
libraryDependencies += "graphframes" % "graphframes" % "0.7.0-spark2.4-s_2.11"

//resolvers +=  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
//
//// one or all from:
//libraryDependencies += "ml.sparkling" %% "sparkling-graph-examples" % "0.0.8-SNAPSHOT"
//libraryDependencies += "ml.sparkling" %% "sparkling-graph-loaders" % "0.0.8-SNAPSHOT"
//libraryDependencies += "ml.sparkling" %% "sparkling-graph-operators" % "0.0.8-SNAPSHOT"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}


