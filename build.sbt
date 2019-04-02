
name := "GenericUnsupervisedMachineLearning"

version := "0.1"

scalaVersion := "2.11.8"


libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1"

libraryDependencies += "org.yaml" % "snakeyaml" % "1.18"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.3.1"


assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
  case "log4j.properties" => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") =>
    MergeStrategy.filterDistinctLines
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}