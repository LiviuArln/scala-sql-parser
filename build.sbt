scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "postgresql" % "postgresql" % "9.1-901-1.jdbc4",
  "org.specs2" %% "specs2" % "2.3.12" % "test",
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4"
)

scalacOptions += "-deprecation"

scalacOptions += "-unchecked"
