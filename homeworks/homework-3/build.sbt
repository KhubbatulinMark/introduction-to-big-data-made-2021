name := "homework-3"

version := "0.1"

scalaVersion := "2.13.6"

libraryDependencies  ++= Seq(
  "org.scalanlp" %% "breeze" % "1.2",
  "org.scalanlp" %% "breeze-viz" % "1.2"
)
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4"
