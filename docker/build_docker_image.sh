#!

DIR=$(dirname $0);

cp $DIR/../target/changesmonitor-3.2.0-standalone.jar . 

docker build -t cmatta/wikipediachangesmonitor:latest -t cmatta/wikipediachangesmonitor:3.2.0 .
docker push cmatta/wikipediachangesmonitor:latest
docker push cmatta/wikipediachangesmonitor:3.2.0

