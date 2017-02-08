# Building and running the MLDB Community Edition Docker image

These instructions are designed for a vanilla installation of **Ubuntu 14.04** and its default compiler, **GCC 4.8** and assume that you have a Github account with [SSH keys](https://help.github.com/categories/ssh/).

It will take around **45 minutes on a 32-core machine with 244GB of RAM** to run through these steps (i.e. on an Amazon EC2 r3.8xlarge instance) and longer on smaller machines. However, you can get up and running in 5 minutes by [using a pre-built Docker images of the MLDB Enterprise Edition](http://mldb.ai/doc/#builtin/Running.md.html) for free with a trial license, which can be obtained instantly by filling out [this form](http://mldb.ai/licensing.html).

## Installing system dependencies via `apt-get`

For C++ code to compile and the Python modules to install correctly, the following system packages need to be installed:

```bash
apt-get install -y \
  git \
  autoconf \
  build-essential \
  language-pack-en \
  libarchive-dev \
  libblas-dev \
  libboost-all-dev \
  libcap-dev \
  libcrypto++-dev \
  libcurl4-openssl-dev \
  libffi-dev \
  libfreetype6-dev \
  libgoogle-perftools-dev \
  liblapack-dev \
  liblzma-dev \
  libpng12-dev \
  libpq-dev \
  libpython-dev \
  libsasl2-dev \
  libssh2-1-dev \
  libtool \
  libyaml-cpp-dev \
  python-virtualenv \
  unzip \
  valgrind
```
## Installing Docker

To build and run the Docker image, you will need to install Docker: https://docs.docker.com/engine/installation/ubuntulinux/

## Cloning, compiling and testing

You will first need to have a Github account with [SSH keys](https://help.github.com/categories/ssh/) set up because the repo uses SSH paths in its submodule configuration. You can test that keys are correctly set up by running the following command and seeing "successfully authenticated":

```bash
ssh -T git@github.com
```

**Note** the `master` branch is bleeding edge and the demos or documentation may be slightly out of sync with the code at any given point in time. To avoid this, it is recommended to build the Community Edition from [the latest tagged release](https://github.com/mldbai/mldb/releases/latest) which is tracked by the `release_latest` branch.

```bash
git clone git@github.com:mldbai/mldb.git
cd mldb
git checkout release_latest
git submodule update --init --recursive
make dependencies
make -k compile
make -k test
```

To speed things up, consider using the `-j` option in `make` to leverage
multiple cores: `make -j8 compile`.

*NOTE* Occasionally, build ordering issues may creep into the build which don't
affect the viability of the build, but may cause `make` to fail.  In that
case, it is acceptable to repeat the `make -k compile` step, which may
successfully complete on a second pass.  (The build order is regression
tested, but the regression tests for the build ordering are run less
frequently than other tests).

*NOTE* Occasionally, tests may fail spuriously, especially due to high load on
the machine when running time-sensitive tests or network issues when
accessing external resources.  Repeating the `make -k test`
step may allow them to pass.  It is OK to use MLDB if the tests don't
all pass; all code merged tagged for release has passed regression
tests in the stable testing environment.

Build output lands in the `build` directory and there is no `make clean`
target: you can just `rm -rf build`. You can speed up recompilation after
deleting your `build` directory by using `ccache`, which can be installed
with `apt-get install ccache`. You can then create a file at the top of the
repo directory called `local.mk` with the following contents:

```
COMPILER_CACHE:=ccache
```

*N.B.* To use `ccache` to maximum effect, you should set the cache size to something like 10GB if you have the disk space with `ccache -M 10G`.

To avoid building MLDB for all supported architectures and save time, check [sample.local.mk](https://github.com/mldbai/mldb/blob/master/jml-build/sample.local.mk)

To have a faster build, you can use clang instead of gcc. Simply add `toolchain=clang` at the end of your make command.

To run a single test, simply specify its name as the target. For python and javascript, include the extension (.py and .js). For C++, omit it.

## Building a Docker image

You'll need to add your user to the `docker` group otherwise you'll need to `sudo` to build the Docker image:

```bash
sudo usermod -a -G docker `whoami`
```

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

  - `quay.io/mldb/mldb_base:14.04`
  - `quay.io/mldb/baseimage:0.9.17`

Some warnings:
* you will not be able to push to quay.io/mldb unless you are a mldb.ai employee
* if you need to rebuild a layer, you must rebuild all layers which depend on it. (all the ones above it)
* the build is done from the top level of the mldb repo
* The build process will build whatever is in the current workspace.
  It does **not** depend on the current state of the upstream Git repo and what
  changes have been pushed.
* **Note that there is no versioning of the resulting images at the moment**


### `quay.io/mldb/mldb_base`

This layer is built on top of `baseimage`, it contains all the required system packages and python modules to run the `mldb` layer.
A change to any of these would require a rebuild of this image:

  - python_requirements.txt
  - python_constraints.txt
  - system package (apt-get)

#### Build instructions

To rebuild this layer, run:

```
make mldb_base IMG_NAME=quay.io/mldb/mldb_base:YOUR_NEW_TAG

make docker_mldb DOCKER_BASE_IMAGE=quay.io/mldb/mldb_base:YOUR_NEW_TAG
# When convinced things are ok:
docker tag quay.io/mldb/mldb_base:YOUR_NEW_TAG quay.io/mldb/mldb_base:vYYYY.MM.DD.0
docker tag quay.io/mldb/mldb_base:YOUR_NEW_TAG quay.io/mldb/mldb_base:14.04
docker push quay.io/mldb/mldb_base:vYYYY.MM.DD.0
docker push quay.io/mldb/mldb_base:14.04
```

The script used to build this layer is `mldb_base/docker_create_mldb_base.sh`

Some switches are available if you need to do a custom build of that layer for some reason:

```
docker_create_mldb_base.sh [-b base_image] [-i image_name] [-w pip_wheelhouse_url]

    -b base_image               Base image to use (quay.io/mldb/baseimage:0.9.17)
    -i image_name               Name of the resulting image (quay.io/mldb/mldb_base:14.04)
    -w pip_wheelhouse_url       URL to use a a pip wheelhouse
```

### `quay.io/mldb/baseimage:0.9.17`

This image is a fork of `phusion/baseimage:0.9.17` rebuilt to have the latest packages updates.
This image contains the base ubuntu packages and a custom init system.

See [phusion/baseimage-docker](https://github.com/phusion/baseimage-docker) for details.

#### Build instructions

If the mldb_base layer does a lot of packages upgrade during its creation, it would be useful to rebuild this layer.
To do so, run the following commands from the top of the mldb repo:

```
docker pull ubuntu:14.04
make baseimage
docker tag quay.io/mldb/baseimage:0.9.17 quay.io/mldb/baseimage:latest
docker push quay.io/mldb/baseimage:latest
```

## S3 Credentials

Some tests require S3 credentials in order to run, as they access public files in the
`public-mldb-ai` S3 bucket.  These credentials are
nothing special: they simply require read-only access to public S3 buckets.
But they need to be enabled for full test coverage.

To enable these tests, you need to create a file ~/.cloud_credentials with a
line with the following format (the fields are tab separated):

```
s3  1   AKRESTOFACCESSKEYID SeCrEtOfKeyGoeShEre
```

The MLDB tests will pick up this file when executing.  The Makefile will check
for the presence of the file containing an S3 line when deciding whether or
not to enable those tests.

## Advanced Topics

These topics should only be used by experienced developers, as they are
not officially supported


### Choosing the toolchain

MLDB is compiled by default using the GCC compiler, version 4.8.


#### Compiling with GCC 5.x or 6.x

In order to use GCC version 5.x or 6.x, the following commands should be used to
install the compiler:

```
sudo add-apt-repository ppa:ubuntu-toolchain-r/test
sudo apt-get update
sudo apt-get install gcc-5 g++-5
sudo apt-get install gcc-6 g++-6
```

You can then add `toolchain=gcc5` or `toolchain=gcc6` to the make command line to use the
newly installed compiler.

#### Compiling with clang

In order to compile with the clang compiler, you will firstly need to
install it:

```
sudo apt-get install clang-3.6
```

You can then add `toolchain=clang` to compile with the clang compiler.

## Environment variables

* `MLDB_CHECK_ROW_SCOPE_TYPES` is a boolean (default 0) that tells MLDB
  whether to do extra type checking of row scopes.  Setting to 1 will
  do so, at the expense of slightly slower code.  It may be helpful in
  debugging of segmentation faults.
* `MLDB_DEFAULT_PATH_OPTIMIZATION_LEVEL` controls how MLDB handles
  optimized code-paths for specific situations.  When set to 'always'
  (the default), optimized paths are always used when available.  This
  leads to maximum speed.  When set to 'never', the base (unoptimized)
  path is used.  When set to 'sometimes', the optimized path is taken
  50% of the time.  This can be used when unit testing to ensure the
  equivalence of optimized and non-optimized versions of code and thus
  verify the correctness of the optimized versions.


## CUDA support

In order to run on a Nvidia GPU, CUDA needs to be enabled and the CUDA
drivers set up on the machine.

### Machine setup

See here: [http://www.r-tutor.com/gpu-computing/cuda-installation/cuda7.5-ubuntu]
The machine needs to have the Nvidia CUDA packages installed on the
machine.

Note that the machine may need to be restarted before the GPUs will be
usable.

```
sudo apt-get install linux-image-generic linux-headers-generic
wget http://developer.download.nvidia.com/compute/cuda/repos/ubuntu1404/x86_64/cuda-repo-ubuntu1404_7.5-18_amd64.deb
sudo dpkg -i cuda-repo-ubuntu1404_7.5-18_amd64.deb
sudo apt-get update
sudo apt-get install cuda
nvidia-smi -q | head  // should print Driver Version: 352.68
```

You will also need cudnn version 5.5 in order to run most models on
GPUS.  To get this you first need to sign up at the Nvidia page
here: [https://developer.nvidia.com/accelerated-computing-developer].

Once the registration is done, you can download the two deb files
for cudnn:

```
31899464 libcudnn5_5.1.3-1+cuda7.5_amd64.deb
28456606 libcudnn5-dev_5.1.3-1+cuda7.5_amd64.deb
```

and install them as follows:

```
sudo dpkg -i libcudnn5_5.1.3-1+cuda7.5_amd64.deb libcudnn5-dev_5.1.3-1+cuda7.5_amd64.deb
```

Note that since cudnn can't be distributed with MLDB, it will need to
be installed inside the MLDB container (but only the first deb file)
so that it can be found at runtime by MLDB.

### Hardware compatibility

Note that MLDB requires cards with shader model > 3.0 in order to run
on CUDA.  You can identify the shader model of your card using the
command

```
nvidia-smi -q
```

### Enabling CUDA in the build

The following should be added to `local.mk`:

```
WITH_CUDA:=1
```

Or the following to the Make command-line:

```
make ... WITH_CUDA=1
```


### Building MLDB for ARM64 (aarch64, eg for Tegra X1 or Jetson X1)

First, the machine needs to be set up with cross compilers:

```
sudo apt-get install \
    g++-aarch64-linux-gnu \
    gcc-aarch64-linux-gnu \
    libc6-arm64-cross \
    libc6-dev-arm64-cross \
    linux-libc-dev-arm64-cross
```

Then we need to modify the system's apt sources.list to add the `ubuntu-ports` repository for the `arm64` architecture:

```
sudo apt-add-repository 'deb [arch=arm64] http://ports.ubuntu.com/ubuntu-ports/ trusty main restricted multiverse universe'
sudo apt-add-repository 'deb [arch=arm64] http://ports.ubuntu.com/ubuntu-ports/ trusty-updates main restricted multiverse universe'
sudo apt-get update
```

Thirdly, we need to download the cross development environment for the
target platform.  This will be installed under `build/aarch64/osdeps`

```
make -j$(nproc) port_deps ARCH=aarch64
```

Fourthly, we need to make the build tools for the host architecture

```
make -j$(nproc) build_tools
```

Finally, we can build the port itself:

```
make -j$(nproc) compile ARCH=aarch64
```

Note that currently no version of the v8 javascript engine is available
from Debian for arch64.  We are working on a solution.


# Building for ARM with hardware float (eg, Raspberry Pi with a Ubuntu derivative)

First, the machine needs to be set up with cross compilers:

```
sudo apt-get install \
    g++-4.8-arm-linux-gnueabihf \
    g++-4.8-multilib-arm-linux-gnueabihf \
    g++-arm-linux-gnueabihf \
    gcc-4.8-arm-linux-gnueabihf \
    gcc-4.8-multilib-arm-linux-gnueabihf \
    gcc-arm-linux-gnueabihf \
    libc6-armhf-cross \
    libc6-dev-armhf-cross \
    linux-libc-dev-armhf-cross
```

Then we need to modify the system's apt sources.list to add the `ubuntu-ports` repository for the `armhf` architecture:

```
sudo apt-add-repository 'deb [arch=armhf] http://ports.ubuntu.com/ubuntu-ports/ trusty main restricted multiverse universe'
sudo apt-add-repository 'deb [arch=armhf] http://ports.ubuntu.com/ubuntu-ports/ trusty-updates main restricted multiverse universe'
sudo apt-get update
```

Thirdly, we need to download the cross development environment for the
target platform.  This will be installed under `build/arm/osdeps`

```
make -j$(nproc) port_deps ARCH=arm
```

Fourthly, we need to make the build tools for the host architecture.  Unfortunately this
takes quite a lot of time as a lot of Tensorflow is required in order to build itself.

```
make -j$(nproc) build_tools  ###  NOTE: do _not_ specify `ARCH=arm` here.  ###
```

Finally, we can build the port itself:

```
make -j$(nproc) compile ARCH=arm
```

The version of MLDB will be placed in `build/arm/bin` and `build/arm/lib`

