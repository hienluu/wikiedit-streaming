name := "wikiedit-streaming"

version := "1.0"

scalaVersion := "2.11.8"

unmanagedBase := baseDirectory.value / "lib"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-streaming" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0"
)

