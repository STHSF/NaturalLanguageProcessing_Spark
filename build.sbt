name := "NaturalLanguageProcessing"

version := "1.0"

scalaVersion := "2.10.4"

// kunyan分词接口
resolvers += "Kunyan Repo" at "http://61.147.114.67:8081/nexus/content/groups/public/"

libraryDependencies += "com.kunyan" % "nlpsuit-package" % "0.2.8.3"

libraryDependencies += "org.scalactic" %% "scalactic" % "2.2.5" % "test"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.5" % "test"

libraryDependencies += "org.scala-lang" % "scala-compiler" % "2.10.4"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.1" excludeAll ExclusionRule(organization = "javax.servlet")

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.7.1" % "provided"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.5.2"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.5.2"

libraryDependencies += "mysql" % "mysql-connector-java" % "3.1.14"

libraryDependencies += "org.graphstream" % "gs-core" % "1.1.2"

libraryDependencies += "org.apache.spark" % "spark-graphx_2.10" % "1.5.2"

libraryDependencies += "com.ibm.icu" % "icu4j" % "56.1"

libraryDependencies += "org.apache.hbase" % "hbase" % "0.98.2-hadoop2"

libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.1.2"

libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.1.2"

libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.1.2"

//libraryDependencies += "org.scalanlp" % "breeze_2.10" % "0.11.2"

libraryDependencies += "org.scalanlp" % "breeze-math_2.10" % "0.4" intransitive()

//libraryDependencies += "org.scalanlp" % "breeze-learn_2.9.2" % "0.2" intransitive()

libraryDependencies += "org.scalanlp" % "breeze-process_2.10" % "0.3" intransitive()

libraryDependencies += "org.scalanlp" % "breeze-viz_2.10" % "0.12" exclude("org.scalanlp", "breeze_2.10")

libraryDependencies += "org.scalanlp" % "nak_2.10" % "1.3"

libraryDependencies += "redis.clients" % "jedis" % "2.8.0"

libraryDependencies += "org.ansj" % "ansj_seg" % "5.0.2"

libraryDependencies += "org.json" % "json" % "20160212"

libraryDependencies += "org.nlpcn" % "nlp-lang" % "1.7"

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("javax", "el", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
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
