name := "Kafka Consumer"

version := "1.0.0"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.kafka" %% "kafka" %"0.8.1" exclude("javax.jms", "jms") exclude("com.sun.jdmk", "jmxtools") exclude("com.sun.jmx", "jmxri")

libraryDependencies += "org.msgpack" % "msgpack" % "0.6.11"

libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.0.0"
