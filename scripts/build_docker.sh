#!/bin/bash

# Direcotry of the script
SCRIPTS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
pushd "${SCRIPTS_DIR}"

## Check for stips docker image and build if it doesn't exist
if [ $(docker image ls | grep stips | wc -l) -eq 0 ]
then
    echo "Made it here"
    # Make temporary directory
    tmp_dir=$(mktemp -d -t stips_docker_XXXXXXX)

    # Go to build directory
    pushd "${tmp_dir}"

    # Clone stips repo into the tmp_dir.
    git clone https://github.com/spacetelescope/STScI-STIPS.git .
    docker build -t stips .

    popd # Go back to starting directory
fi

pushd ..
docker build -t wings .

popd
