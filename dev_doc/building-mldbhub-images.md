Building and deploying a new MLDB hub docker image
==================================================

First, we need to build MLDB from the git tag/commit we want to use.
This is usually done from khan.mldb.lan.

```
VERSION="2016.04.04.0"
TAG=v${VERSION}

git fetch --tags && git checkout $TAG
git submodule update --init --recursive

nice make -j $(nproc) docker_mldbhub VERSION_NAME="$VERSION"
```

Then, tag the new image with 'staging' and push it to spacedock:
```
docker tag spacedock.mldb.ai/mldb/mldbhub:${USER}_latest spacedock.mldb.ai/mldb/mldbhub:staging
docker tag spacedock.mldb.ai/mldb/mldbhub:staging spacedock.mldb.ai/mldb/mldbhub:${TAG}
docker push spacedock.mldb.ai/mldb/mldbhub:staging
docker push spacedock.mldb.ai/mldb/mldbhub:${TAG}
```

Pushing to staging environment
------------------------------

On dockerhost-dev1.mldb.ai, pull the new image:

```
docker pull spacedock.mldb.ai/mldb/mldbhub:staging
```

Then POST {} to  `/api/pool/schedule_recycle`, so that tmpmldb will recycle unused containers and start new ones using the new image:

```
curl -X POST -H 'Authorization: token THE_API_TOKEN' https://dockerhost-dev1.mldb.ai:1042/api/pool/schedule_recycle
```

To watch the progress of the container respawn, check the logs using this [link](http://elk1.mldb.ai:5601/app/kibana#/discover?_g=(refreshInterval:(display:Off,pause:!f,value:0),time:(from:now-15m,mode:quick,to:now))&_a=(columns:!(message,hostname),filters:!(),index:%5Blogstash-%5DYYYY.MM.DD,interval:auto,query:(query_string:(analyze_wildcard:!t,query:'customer.raw:mldbhub-dev%20AND%20program.raw:(tmpmldb%20OR%20docker*)')),sort:!('@timestamp',desc),vis:(aggs:!((params:(field:hostname,orderBy:'2',size:20),schema:segment,type:terms),(id:'2',schema:metric,type:count)),type:histogram))&indexPattern=%5Blogstash-%5DYYYY.MM.DD&type=histogram)


Pushing to PRODUCTION environment
---------------------------------

Once we're confident it is a good release, we need to tag the image with the 'prod' tag.

On the build machine:
```
docker tag spacedock.mldb.ai/mldb/mldbhub:staging spacedock.mldb.ai/mldb/mldbhub:prod
docker push spacedock.mldb.ai/mldb/mldbhub:prod
```

Then pull the image on all prod dockerhost (dockerhost1.mldb.ai, dockerhost2.mldb.ai):
```
docker pull spacedock.mldb.ai/mldb/mldbhub:prod
```

Once the image is pulled, send a `/api/pool/schedule_recycle` API call to tmpmldb. (see above)

To watch the progress of the container respawn, you can use the following [link](http://elk1.mldb.ai:5601/app/kibana#/discover?_g=(refreshInterval:(display:Off,pause:!f,value:0),time:(from:now-15m,mode:quick,to:now))&_a=(columns:!(message,hostname),filters:!(),index:%5Blogstash-%5DYYYY.MM.DD,interval:auto,query:(query_string:(analyze_wildcard:!t,query:'customer.raw:mldbhub%20AND%20program.raw:(tmpmldb%20OR%20docker*)')),sort:!('@timestamp',desc),vis:(aggs:!((params:(field:hostname,orderBy:'2',size:20),schema:segment,type:terms),(id:'2',schema:metric,type:count)),type:histogram))&indexPattern=%5Blogstash-%5DYYYY.MM.DD&type=histogram)

  - query string: program.raw:tmpmldb AND customer.raw:"mldbhub"

It is also possible to watch the status of the dockerhost by checking this url: https://dockerhost1.mldb.ai:1042/api/stats

