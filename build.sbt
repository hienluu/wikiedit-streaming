name := "wikiedit-streaming"

lazy val commonSettings = Seq(
  version := "0.0.2",
  organization := "org.wikiedit-streaming",
  scalaVersion := "2.11.8"
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

lazy val receiver = project.in(file("streaming-receiver")).
                        settings(commonSettings,
                          libraryDependencies ++= commonDependencies)

lazy val examples = project.in(file("examples")).
                          dependsOn(receiver).
                          settings(commonSettings,
                            libraryDependencies ++= commonDependencies
                          )


lazy val commonDependencies = Seq(
  "org.apache.spark" %% "spark-core" % "2.2.1" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.2.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.2.1" % "provided",
  "org.apache.spark" %% "spark-catalyst" % "2.2.1" % "provided"
)