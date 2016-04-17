val kafkaVersion = "0.8.2.0"

val zookeeperVersion = "3.4.6"

libraryDependencies += "org.apache.kafka" %% "kafka" % kafkaVersion exclude("javax.jms", "jms") exclude("com.sun.jmx", "jmxri") exclude("com.sun.jdmk", "jmxtools") withSources()

libraryDependencies += "org.apache.kafka" % "kafka-clients" % kafkaVersion exclude("javax.jms", "jms") exclude("com.sun.jmx", "jmxri") exclude("com.sun.jdmk", "jmxtools") withSources()

libraryDependencies += "org.apache.zookeeper" % "zookeeper" % zookeeperVersion withSources()

retrieveManaged := true
