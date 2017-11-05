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

# Useful commands:

mvn clean install -nsu && ./build_mochi_docker.sh && ./start_mochi_docker.sh
