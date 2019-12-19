name := "spark-streaming-greenplum-example"
version := "1.0.0"
scalaVersion := "2.11.12"
mainClass in Compile := Some("StreamingOmnichannelChatGp")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.4",
  "org.apache.spark" %% "spark-sql" % "2.4.4"
)