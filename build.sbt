name := "Link"

version := "0.1.1"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "com.twitter" %% "finagle-core" % "6.18.0",
  "org.scalatest" % "scalatest_2.10" % "2.2.0" % "test"
)

publishMavenStyle := true

publishArtifact := true

homepage := Some(url("https://github.com/reneweb/Link"))

organization := "com.github.reneweb"

//Github https://github.com/reneweb/mvn-repo
publishTo := Some(Resolver.file("localDirectory", file(Path.userHome.absolutePath + "/mvn-repo" )))
