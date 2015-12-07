# Building and running a development Docker image

These instructions are designed for a clean installation of **Ubuntu 14.04** and its default compiler, **GCC 4.8**.

## System dependencies

For C++ code to compile, the following system packages need to be installed:

```bash
apt-get install -y git valgrind build-essential libboost-all-dev libgoogle-perftools-dev liblzma-dev libcrypto++-dev libblas-dev liblapack-dev python-virtualenv libcurl4-openssl-dev libssh2-1-dev libpython-dev libgit2-dev libv8-dev libarchive-dev
```

For Python modules to install correctly:

```bash
apt-get install -y libffi-dev libfreetype6-dev libpng12-dev libcap-dev
```

To build and run the Docker image, you will need to install Docker: https://docs.docker.com/engine/installation/ubuntulinux/

## Cloning, compiling and test

```bash
git clone git@github.com:datacratic/mldb.git
cd mldb
git submodule update --init
make dependencies
make compile
make test
```

To speed things up, consider using the `-j` option in `make` to leverage multiple cores.

Build output lands in the `build` directory and there is no `make clean` target: you can just `rm -rf build`. You can speed up recompilation after deleting your `build` directory by using `ccache`, which can be installed with `apt-get install ccache`. You can then create a file at the top of the repo directory called `local.mk` with the following contents:

```
CXX := ccache g++
CC := ccache gcc
```

*N.B.* To use `ccache` to maximum effect, you should set the cache size to something like 10GB if you have the disk space with `ccache -M 10G`.

## Building a Docker image

To *build* a development Docker image just run the following command from the top level of this repo:

```bash
nice make -j16 -k docker_mldb DOCKER_ALLOW_DIRTY=1
```

The final lines of output will give you a docker hash for this image, and the image is also tagged as `<username>_latest` where `<username>` is your Unix username on the box.

To *run* a development Docker image you just built, follow the Docker instructions from http://mldb.ai/doc/#builtin/Running.md.html except where the tag there is `latest` just substitute `<username>_latest` and where the container name there is `mldb` just substitute something unique to you (e.g. `<username>` is a good candidate!).

Docker images built this way will have the internal/experimental entities shown in the documentation.  For external releases, the flags `RUN_STRIP=-s` is passed which, as a side effect, will hide the internal entities in the documentation.

## Basic Build System Commands

These all work from the top-level of this repo:

* `make compile` will compile all libraries, executables and tests.
* `make test` will execute all tests
* the `-k` flag will prevent `make` from stopping the first time it hits an error
* the `-j<x>` flag will cause `make` to use `<x>` cores to build
* `makerun <prog> <args>` will cause the build system to rebuild and run program `<prog>` with arguments `<args>` *so long as* you put the following into your `.bashrc` or `.profile` or equivalent:

```
makerun() {
    name=$1
    shift
    make -j8 run_$name "$name"_ARGS="$*"
}
```

## Basic Docker commands

To see the running containers (with `-a` to see _all_ the containers):
```bash
docker ps
```

To see all the images (with `--no_trunc` to see the full image IDs):
```bash
docker images
```

To kill a running container:
```bash
docker kill <container's name>
```

To erase a killed container:
```bash
docker rm <container's name>
```

To delete an image. Note that you may have a conflict and need to delete a container before deleting an image.
```bash
docker rmi <full image ID>
```

## Advanced Docker

The easiest way to get a privileged shell into a running container is to use `docker exec`:

```
docker run ...  # start your container as explained at http://mldb.ai/doc/#builtin/Running.md.html
docker exec -t -i CONTAINER_ID|CONTAINER_NAME /bin/bash
```

## Docker layers

The `mldb` docker is built on top of a few base images:

  - `quay.io/datacratic/mldb_base:14.04`
  - `quay.io/datacratic/baseimage:0.9.17`

Some warnings:
* you will not be able to push to quay.io/datacratic unless you are a Datacratic employee
* if you need to rebuild a layer, you must rebuild all layers which depend on it. (all the ones above it)
* the build is done from the top level of the mldb repo
* The build process will build whatever is in the current workspace.
  It does **not** depend on the current state of the upstream Git repo and what
  changes have been pushed.
* **Note that there is no versioning of the resulting images at the moment**


### `quay.io/datacratic/mldb_base`

This layer is built on top of `baseimage`, it contains all the required system packages and python modules to run the `mldb` layer.
A change to any of these would require a rebuild of this image:

  - python_requirements.txt
  - python_constraints.txt
  - system package (apt-get)

#### Build instructions

To rebuild this layer, run:

```
make mldb_base
docker push quay.io/datacratic/mldb_base:14.04
```

The script used to build this layer is `mldb_base/docker_create_mldb_base.sh`

A few things to keep in mind when editing/running the script:

Some switches are available if you need to do a custom build of that layer for some reason:

```
docker_create_mldb_base.sh [-b base_image] [-i image_name] [-w pip_wheelhouse_url]

    -b base_image               Base image to use (quay.io/datacratic/baseimage:0.9.17)
    -i image_name               Name of the resulting image (quay.io/datacratic/mldb_base:14.04)
    -w pip_wheelhouse_url       URL to use a a pip wheelhouse
```

### `quay.io/datacratic/baseimage:0.9.17`

This image is a fork of `phusion/baseimage:0.9.17` rebuilt to have the latest packages updates.
This image contains the base ubuntu packages and a custom init system.

See [phusion/baseimage-docker](https://github.com/phusion/baseimage-docker) for details.

#### Build instructions

If the mldb_base layer does a lot of packages upgrade during its creation, it would be useful to rebuild this layer.
To do so, run the following commands from the top of the mldb repo:

```
make baseimage
docker push quay.io/datacratic/baseimage:latest
```

## S3 Credentials

Some tests require S3 credentials in order to run.  These credentials are
nothing special---they simply require read-only access to public S3 buckets.
But they need to be enabled for full test coverage.

To enable these tests, you need to create a file ~/.cloud_credentials with a
line with the following format (the fields are tab separated):

```
s3  1   AKRESTOFACCESSKEYID SeCrEtOfKeyGoeShEre
```

The MLDB tests will pick up this file when executing.  The Makefile will check
for the presence of the file containing an S3 line when deciding whether or
not to enable those tests.
