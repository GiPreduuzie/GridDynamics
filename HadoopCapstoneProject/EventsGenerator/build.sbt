name := "EventsGenerator"

version := "0.1"

scalaVersion := "2.13.1"

// https://mvnrepository.com/artifact/joda-time/joda-time
libraryDependencies += "joda-time" % "joda-time" % "2.10.5"

// https://mvnrepository.com/artifact/com.github.andyglow/websocket-scala-client
libraryDependencies += "com.github.andyglow" %% "websocket-scala-client" % "0.3.0"

assemblyMergeStrategy in assembly := {
  case "META-INF/io.netty.versions.properties" => MergeStrategy.concat
//  case "META-INF/INDEX.LIST" => MergeStrategy.concat
//  case "META-INF/MANIFEST.MF" => MergeStrategy.concat
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

