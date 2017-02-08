Runtime dependencies / NVIDIA driver versions
=============================================

It is required to use the same NVIDIA driver version on both the docker host and the docker container.

*This Dockerfile will install version 375 of the driver.*


MLDB base docker image with NVIDIA libraries (cuda, cudnn, etc...)
==================================================================

The Dockerfile in this folder can be used to create a MLDB *baseimage* which will contain all required libraries to access the NVIDIA GPUs on a docker host.

Caveats and requirement are outlined below.

Requirements
------------

### mldb_base

This baseimage should is built on top of `quay.io/mldb/mldb_base`.

Make sure it exists before trying to build. (`docker pull quay.io/mldb/mldb_base:14.04`)

### NVIDIA packages

3 files with distribution restrictions are required before starting the build.
They can be obtained on the NVIDIA website at the URL mentioned below:

  - `cuda-repo-ubuntu1404-8-0-local_8.0.44-1_amd64.deb`: https://developer.nvidia.com/compute/cuda/8.0/prod/local_installers/cuda-repo-ubuntu1404-8-0-local_8.0.44-1_amd64-deb
  - `libcudnn5_5.1.5-1+cuda8.0_amd64.deb`: https://developer.nvidia.com/compute/machine-learning/cudnn/secure/v5.1/prod/8.0/libcudnn5_5.1.5-1+cuda8.0_amd64-deb
  - `libcudnn5-dev_5.1.5-1+cuda8.0_amd64.deb`: https://developer.nvidia.com/compute/machine-learning/cudnn/secure/v5.1/prod/8.0/libcudnn5-dev_5.1.5-1+cuda8.0_amd64-deb 

*The cuDNN downloads require a free NVIDIA Developper subscription.*

Put these 3 files in the `container_files/nvidia/files` directory.

### Building the baseimage

Run `docker build` from the `container_files/nvidia` directory as follows,  changing `TAG` for something useful:

```
docker build -t quay.io/mldb/mldb_base_nvidia:TAG -f ./Dockerfile  .

# To reduce the image size we can squash (flatten, merge) the docker image
virtualenv/bin/docker-squash -t quay.io/mldb/mldb_base_nvidia:TAG.squashed quay.io/mldb/mldb_base_nvidia:TAG
```


### Building MLDB docker based on this image

```
nice make -j $(nproc) docker_mldb WITH_CUDA=1 DOCKER_POST_INSTALL_ARGS=-s DOCKER_BASE_IMAGE=quay.io/mldb/mldb_base_nvidia:TAG.squashed
```

Running MLDB with GPUs
======================

Once the MLDB docker is built, it can be started as follows.

See the notes at the top of this document for runtime restrictions on the driver version.


```
docker run [... usual arguments ...] \
    --device=/dev/nvidia0:/dev/nvidia0 \
    --device=/dev/nvidiactl:/dev/nvidiactl \
    --device=/dev/nvidia-uvm:/dev/nvidia-uvm \
    quay.io/mldb/mldb:YOUR_MLDB_TAG
```

*Multiple GPUs can be mounted into the container by passing multiple `--dev /dev/nvidiaN:/dev/nvidiaN` flags.*
