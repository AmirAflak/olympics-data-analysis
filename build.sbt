lazy val root = project
  .in(file("."))
  .settings(
    name := "olympic_analytics",
    version := "0.1.0-SNAPSHOT",
    scalaVersion := "2.13.12",
    
    libraryDependencies += "org.scalameta" %% "munit" % "1.0.0" % Test,
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.1",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.1",
    libraryDependencies += "org.postgresql" % "postgresql" % "42.7.3",
    libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.5.1",




    fork := true,
    javaOptions += "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"

  )