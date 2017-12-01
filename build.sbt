name := "graphVisualize"

version := "0.1"

scalaVersion := "2.11.10"

// Graph Visualization

libraryDependencies += "org.graphstream" % "gs-core" % "1.2"
libraryDependencies += "org.graphstream" % "gs-ui" % "1.2"
//
libraryDependencies += "org.scalanlp" %% "breeze" % "0.12"
libraryDependencies += "org.scalanlp" %% "breeze-viz" % "0.12"
//
libraryDependencies += "org.jfree" % "jcommon" % "1.0.24"
//
libraryDependencies += "org.jfree" % "jfreechart" % "1.0.19"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0" % "provided"

// https://mvnrepository.com/artifact/org.apache.spark/spark-graphx_2.10
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.2.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib_2.11
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.2.0" % "provided"


