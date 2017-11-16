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

Protobuf 3.4.0 Installation 


Ubuntu 16.04


curl -OL https://github.com/google/protobuf/releases/download/v3.4.0/protoc-3.4.0-linux-x86_64.zip


unzip protoc-3.4.0-linux-x86_64.zip -d proto3.4


sudo mv protoc3.4/bin/* /usr/local/bin/


sudo mv protoc3.4/include/* /usr/local/include/


protoc --version


libprotoc 3.4.0


MacOS


Install protobuf - https://stackoverflow.com/questions/21775151/installing-google-protocol-buffers-on-mac

Generate proto - /usr/local/bin/protoc ./src/main/java/edu/stanford/cs244b/mochi/server/messages/MochiProtocol.proto --java_out=src/main/java/

# Installing JDK, Maven and Eclipse in Windows

# Installing Docker in Windows

# AWS notes

* Install AWS cli on your Mac, Cygwin :


pip install awscli


* After installing AWS cli, run  "aws configure" and update your access and secret key in the field provided


* Then run : aws ecr get-login --region us-west-1


* You would an access token that is valid for 12 hours


* You would have to docker login using that access token


* Once login is successful, we can now create a docker tag that matches our repository

sudo docker tag mochi-db:latest <account-id>.dkr.ecr.us-west-1.amazonaws.com/mochi-db:latest


* Push our docker image to AWS repository


sudo docker push <account-id>.dkr.ecr.us-west-1.amazonaws.com/mochi-db:latest


* On our ECS instance, we have to docker login to our AWS repository following the same steps


* Pull our image

docker pull <account-id>.dkr.ecr.us-west-1.amazonaws.com/mochi-db:latest

* Run docker on ECS
