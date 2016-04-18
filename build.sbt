resolvers += "confluent" at "http://packages.confluent.io/maven/"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.9.1.0-cp1"

libraryDependencies += "org.apache.kafka" % "kafka-streams" % "0.9.1.0-cp1"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.6.1"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.6.1"

retrieveManaged := true
