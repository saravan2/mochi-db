#!/bin/bash -ex
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

demo_config_file="${DIR}/config/sample_config"
[[ -f "${demo_config_file}" ]]  || echo "Missing config!!!"
cur_s="server-ed25bc93-1047-4242-b87b-2246355b020b"
java -jar -DclusterConfig="${demo_config_file}" -DclusterCurrentServer="${cur_s}" "${DIR}/target/mochi-db-0.1.0.jar"
