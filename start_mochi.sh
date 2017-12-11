#!/bin/bash -ex
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

demo_config_file="${DIR}/config/sample_config"
[[ -f "${demo_config_file}" ]]  || echo "Missing config!!!"
java -jar -DclusterConfig="${demo_config_file}" "${DIR}/target/mochi-db-0.1.0.jar"
