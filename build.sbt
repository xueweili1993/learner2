name := "learner2"

version := "1.0"

scalaVersion := "2.11.8"


libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1" % "provided"


// http://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk

libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.11.3"



resolvers += "Akka Repository" at "http://repo.akka.io/releases/"