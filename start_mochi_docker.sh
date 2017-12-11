#!/bin/bash -ex

NODE_NAME="${1}"

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
demo_config_file="${DIR}/config/sample_config"
[[ -f "${demo_config_file}" ]]  || echo "Missing config!!!"

case "${NODE_NAME}" in
  a) cur_s="server-ed25bc93-1047-4242-b87b-2246355b020b" ; index=1 ;;
  b) cur_s="server-55a78d3f-783d-43ae-95c1-6d0f5f02fe0c" ; index=2 ;;
  c) cur_s="server-6c023c90-87ed-40d9-8f38-48cb03fa2135" ; index=3 ;;
  d) cur_s="server-6a3b63b2-9fc8-4f3d-97c1-0f61cb244a0c" ; index=4 ;;
  e) cur_s="server-20d27225-0253-4013-8f6c-b32afb3e5452" ; index=5 ;;
  *) echo "Please specify which server you are starting" ; exit 1 ;;
esac

http_port=$((8080+${index}))
netty_port=$((8000+${index}))

cp "${demo_config_file}" /tmp/mochi_sample_config
docker run -it -e CLUSTER_CURRENT_SERVER="${cur_s}" -e CLUSTER_CONFIG="/mochi_config/mochi_sample_config" -v "/tmp/mochi_sample_config:/mochi_config/mochi_sample_config" \
           -p "${http_port}:8080" -p "${netty_port}:8081" mochi-db:latest
