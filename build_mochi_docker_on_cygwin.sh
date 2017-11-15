#!/bin/bash -ex
commit_count=$(git rev-list HEAD --count)
mochi_docker_tag=mochi-db:0.1.0-${commit_count}-latest
rm -rf "target/Dockerfile_server"
cp "Dockerfile_server" "target/Dockerfile_server"
docker build -f "target/Dockerfile_server" -t  "${mochi_docker_tag}" "target"
docker tag "${mochi_docker_tag}" "mochi-db:latest"

echo "Docker image build"
docker images "${mochi_docker_tag}"
