scalaVersion := "2.13.18"

lazy val root = rootProject
  .settings(
    name := "datom-prolly-tree",
    libraryDependencies ++= Seq(
      //You can add library dependencies here, for example,
      //"org.scalatest" %% "scalatest" % "3.2.19" % Test,
      //"org.scalameta" %% "munit" % "1.2.3" % Test
    )
  )

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.17" % Test