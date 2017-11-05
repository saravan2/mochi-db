# mochi-db
Dynamic Byzantine Fault Tolerant Distributed Key Value Database



# Building
mvn clean install

# Runing
1. start_mochi.sh
2. goto http://localhost:8080/

# Building docker image
### Testing docker works:
check_docker_run.sh

### Building docker image
build_mochi_docker.sh

### Running mochi docker
start_mochi_docker.sh

# Useful commands/Notes:

Maven installation errors :
As of 11/5/2017 In Ubuntu 16.04, apt-get install maven partially installs java-8-openjdk-amd64

This partiall installation causes grief when building plugins

We can fix java 8 via sudo apt-get install openjdk-8-jdk

More info on : https://stackoverflow.com/questions/26313902/maven-error-perhaps-you-are-running-on-a-jre-rather-than-a-jdk

Read pom.xml file to know Java version dependencies before attempting mvn install
<properties>
       <java.version>1.8</java.version>
 </properties>


mvn clean install -nsu && ./build_mochi_docker.sh && ./start_mochi_docker.sh

Install protobuf - https://stackoverflow.com/questions/21775151/installing-google-protocol-buffers-on-mac

Generate proto - /usr/local/bin/protoc ./src/main/java/edu/stanford/cs244b/mochi/server/messages/MochiProtocol.proto --java_out=src/main/java/

