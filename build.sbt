mainClass in (Compile, packageBin) := Some("com.kunyan.userportrait.temp.ys.CombineResult")

name := "personas"

version := "1.0"

scalaVersion := "2.10.4"

resolvers += "Kunyan Repo" at "http://222.73.34.92:8081/nexus/content/groups/public/"

libraryDependencies += "org.jsoup" % "jsoup" % "1.8.3"

dependencyOverrides += "org.slf4j" % "slf4j-log4j12" % "1.7.6"

libraryDependencies += "org.scala-lang" % "scala-compiler" % "2.10.4" % "provided" excludeAll ExclusionRule(organization = "javax.servlet")

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.1" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.1" % "provided"

libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.4" % "provided"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.5.2"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.5.2" % "provided" excludeAll ExclusionRule(organization = "javax.servlet")

libraryDependencies += "org.apache.spark" % "spark-hive_2.10" % "1.5.2"

libraryDependencies += "org.apache.kafka" % "kafka_2.10" % "0.8.2.1" % "provided"

libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.1.2"

libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.1.2"

libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.1.2"

libraryDependencies += "org.scalactic" %% "scalactic" % "2.2.5" % "provided"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.5" % "test"

libraryDependencies += "org.datanucleus" % "datanucleus-core" % "3.2.10" % "provided"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.38"

libraryDependencies += "com.google.guava" % "guava" % "14.0.1"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.5.2"

libraryDependencies += "org.json" % "json" % "20140107"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.5.2"

libraryDependencies += "org.apache.spark" % "spark-graphx_2.10" % "1.5.2"

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("javax", "el", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("org", "slf4j", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "plugin.xml" => MergeStrategy.rename
  case "overview.html" => MergeStrategy.rename
  case "parquet.thrift" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

test in assembly := {}
