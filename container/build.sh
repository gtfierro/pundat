#!/bin/bash

set -x

# build pundat
cd ..
go build
if [ $? != 0 ]; then
    echo "Error building pundat"
    exit 1
fi
cd -
cp ../pundat pundat
docker build --rm -t gtfierro/pundat .
if [ $? != 0 ]; then
    echo "Error building container"
    exit 1
fi

docker push gtfierro/pundat
if [ $? != 0 ]; then
    echo "Error pushing container"
    exit 1
fi
