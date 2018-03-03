name := "wikiedit-streaming"


//unmanagedBase := baseDirectory.value / "lib"

lazy val commonSettings = Seq(
  version := "0.0.1",
  organization := "org.wikiedit-streaming",
  scalaVersion := "2.11.8"
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

lazy val receiver = project.in(file("streaming-receiver")).
                        settings(commonSettings: _*)

lazy val examples = project.in(file("examples")).
                          dependsOn(receiver).
                          settings(commonSettings: _*)
