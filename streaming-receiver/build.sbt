
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.1" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.2.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.2.1" % "provided",
  "org.apache.spark" %% "spark-catalyst" % "2.2.1" % "provided"
)