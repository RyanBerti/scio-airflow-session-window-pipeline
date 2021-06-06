import sbt._
import Keys._
val scioVersion = "0.10.3"
val beamVersion = "2.28.0"
lazy val commonSettings = Def.settings(
  organization := "RyanBerti",
  // Semantic versioning http://semver.org/
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.13.3",
  scalacOptions ++= Seq("-target:jvm-1.8",
                        "-deprecation",
                        "-feature",
                        "-unchecked"),
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
)

lazy val root: Project = project
  .in(file("."))
  .settings(commonSettings)
  .settings(
    name := "scio-airflow-session-window-pipeline",
    description := "scio-airflow-session-window-pipeline",
    publish / skip := true,
    libraryDependencies ++= Seq(

      // default imports from the gitter repo
      "com.spotify" %% "scio-core" % scioVersion,
      "com.spotify" %% "scio-test" % scioVersion % Test,
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
      "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion,
      "org.slf4j" % "slf4j-simple" % "1.7.30",

      // additional imports from scio
      "com.spotify" %% "scio-google-cloud-platform" % scioVersion,
      "com.spotify" %% "scio-parquet" % scioVersion,

      // additional imports for proto support
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf",

      // additional imports for avro support
      "com.spotify" %% "magnolify-avro" % "0.4.3",

      // additional imports for using ParquetIO
      "org.apache.beam" % "beam-sdks-java-io-parquet" % beamVersion,

      // additional imports for pubsub client
      "com.google.cloud" % "google-cloud-pubsub" % "1.112.1",

      // additional imports for json support
      "io.circe" %% "circe-core" % "0.12.3",
      "io.circe" %% "circe-generic" % "0.12.3",
      "io.circe" %% "circe-parser" % "0.12.3"

    )
  )
  .enablePlugins(JavaAppPackaging)

// included to support compiling protos, generates both scala and java
// see https://spotify.github.io/scio/io/Protobuf.html#scalapb
Compile / PB.targets := Seq(
  PB.gens.java -> (Compile / sourceManaged).value,
  scalapb.gen(javaConversions = true) -> (Compile / sourceManaged).value
)

// Included to fix guava transitive dependency issue
// see https://github.com/spotify/scio/issues/2945
ThisBuild / dependencyOverrides ++= Seq(
  "com.google.guava" % "guava" % "30.1.1-jre"
)

// Included due to some local env issue I was having trouble fixing
// https://github.com/spotify/scio/issues/3230
run / fork := true

