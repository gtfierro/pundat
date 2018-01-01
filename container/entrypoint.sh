#!/bin/bash

set -e



echo "ARG $1"
if [[ $1 = "bash" || $1 = "shell" ]]
then
  set +ex
  bash -i
  exit 0
fi

if [ -z "$BTRDB_SERVER" ]
then
  echo "The environment variable BTRDB_SERVER must be set"
  exit 1
fi

if [ -z "$MONGO_SERVER" ]
then
  echo "The environment variable MONGO_SERVER must be set"
  exit 1
fi

if [ -z "$GILES_BW_ENTITY" ]
then
  echo "The environment variable GILES_BW_ENTITY must be set"
  exit 1
fi

if [ -z "$GILES_BW_NAMESPACE" ]
then
  echo "The environment variable GILES_BW_NAMESPACE must be set"
  exit 1
fi

if [ -z "$COLLECTION_PREFIX" ]
then
  echo "The environment variable COLLECTION_PREFIX must be set"
  exit 1
fi

if [ -z "$GILES_BW_ADDRESS" ]
then
  GILES_BW_ADDRESS=localhost:28589
fi

LISTEN_NAMESPACES=""
for ns in $GILES_BW_LISTEN; do
LISTEN_NAMESPACES+=ListenNS=$ns$'\n'
done

cat >pundat.ini <<EOF
[Archiver]
PeriodicReport = true
BlockExpiry = 10s

[BOSSWAVE]
Address = ${GILES_BW_ADDRESS}
Entityfile = ${GILES_BW_ENTITY}
DeployNS = ${GILES_BW_NAMESPACE}
${LISTEN_NAMESPACES}

[Metadata]
Address = ${MONGO_SERVER}
CollectionPrefix = ${COLLECTION_PREFIX}

[BtrDB]
Address = ${BTRDB_SERVER}
EOF

cat pundat.ini
trap 'kill -TERM $PID' TERM INT
pundat archiver -c pundat.ini &
PID=$!
wait $PID
trap - TERM INT
wait $PID
EXIT_STATUS=$?
