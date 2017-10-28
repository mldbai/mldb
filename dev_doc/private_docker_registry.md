# Private docker image registry: spacedock.mldb.ai

We have a place to host custom docker image:  spacedock.mldb.ai

We can use this new 'registry' to upload docker images that are meant to be used outside of the development environment,
but that should not be made public.

A typical use case would be to run a custom/test MLDB build on a docker host which cannot build MLDB (such as Ubuntu 12.04 hosts)

## Caveats

A few caveats:
- For the moment, images will be erased from the registry 1 week after they have been uploaded.
  We can revisit this later, based on our usage.
- There is no way to easily 'browse' the registry, you need to know which image / tag you want when you `docker pull` it.
- There is no authentication, so this service cannot be made available outside mldb's network.

# Pushing images to spacedock

There are 2 ways to push a MLDB build to the registry:

## Building a new image:

```
nice make -j $(nproc) docker_mldb DOCKER_REGISTRY=spacedock.mldb.ai/ DOCKER_TAG=the_tag_you_want
docker push spacedock.mldb.ai/mldb/mldb:the_tag_you_want
```

## 'retagging' an existing image:

```
docker tag quay.io/mldb/mldb:existing_tag spacedock.mldb.ai/mldb/mldb:the_tag_you_want
docker push spacedock.mldb.ai/mldb/mldb:the_tag_you_want
```

# Pulling the image from spacedock

Once the image has been uploaded, a fellow developer can pull it from any machine that can reach `spacedock.mldb.ai` via the following commands:

```
docker pull spacedock.mldb.ai/mldb/mldb:the_tag_you_want
docker run [...usual options...] spacedock.mldb.ai/mldb/mldb:the_tag_you_want
```
