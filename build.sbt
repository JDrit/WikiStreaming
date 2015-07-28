
name := "Streaming"

version := "1.0"

organization := "edu.rit.csh"

scalaVersion := "2.10.4"

sparkVersion := "1.4.0"

sparkComponents := Seq("core", "sql", "streaming")

libraryDependencies ++= Seq("org.pircbotx" % "pircbotx" % "2.0.1",
                            "org.scalaj" % "scalaj-http_2.10" % "1.1.5",
                            "org.json4s" %% "json4s-native" % "3.3.0.RC3")