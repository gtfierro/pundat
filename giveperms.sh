#!/bin/bash

set -u

if [ -z ${1+x} ]; then
    echo "Usage: ./giveperms.sh from to on"
    exit 1
fi

fromEntity=$1
toKey=$2
archiver=$3
toEntity=$(bw2 i $toKey | grep "VK:" -m 1 | cut -d' ' -f4)
alteredTo=${toEntity::-1}

echo "From $fromEntity"
echo "To $toEntity"
echo "On archiver $archiver"
echo $alteredTo

uri=$archiver/s.giles/+/lastalive
echo "Checking C to $uri"
bw2 bc -t $toEntity -u $uri -x C
if [ $? != 0 ]; then
    echo "Granting C to $toEntity on $uri. Is this okay?"
    echo "Waiting 5 sec..."
    sleep 5
    bw2 mkdot -e 2d -m "Can see archiver liveness" -f $fromEntity -t $toEntity -u $uri -x C
fi

uri=$archiver/s.giles/_/i.archiver/slot/query
echo "Checking P to $uri"
bw2 bc -t $toEntity -u $uri -x P
if [ $? != 0 ]; then
    echo "Granting P to $toEntity on $uri. Is this okay?"
    echo "Waiting 5 sec..."
    sleep 5
    bw2 mkdot -e 2d -m "Pub query access to $archiver archiver" -f $fromEntity -t $toEntity -u $uri -x P
fi

uri=$archiver/s.giles/_/i.archiver/signal/$alteredTo,queries
echo "Checking C to $uri"
bw2 bc -t $toEntity -u $uri -x C
if [ $? != 0 ]; then
    echo "Granting C to $toEntity on $uri. Is this okay?"
    echo "Waiting 5 sec..."
    sleep 5
    bw2 mkdot -e 2d -m "Sub query access to $archiver archiver" -f $fromEntity -t $toEntity -u $uri -x C
fi
