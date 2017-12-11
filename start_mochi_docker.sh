#!/bin/bash -ex

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
demo_config_file="${DIR}/config/sample_config"
[[ -f "${demo_config_file}" ]]  || echo "Missing config!!!"
cur_s="server-ed25bc93-1047-4242-b87b-2246355b020b"

cp "${demo_config_file}" /tmp/mochi_sample_config
docker run -it -e CLUSTER_CURRENT_SERVER="${cur_s}" -e CLUSTER_CONFIG="/mochi_config/mochi_sample_config" -v "/tmp/mochi_sample_config:/mochi_config/mochi_sample_config" \
           -p 8080:8080 -p 8081:8081 mochi-db:latest
