#!/bin/bash -ex
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

commit_count=$(git rev-list HEAD --count)
mochi_docker_tag=mochi-db:0.1.0-${commit_count}-latest
rm -rf "${DIR}/target/Dockerfile_server"
cp "${DIR}/Dockerfile_server" "${DIR}/target/Dockerfile_server"
docker build -f "${DIR}/target/Dockerfile_server" -t  "${mochi_docker_tag}" "${DIR}/target"
docker tag "${mochi_docker_tag}" "mochi-db:latest"

echo "Docker image build"
docker images "${mochi_docker_tag}"

