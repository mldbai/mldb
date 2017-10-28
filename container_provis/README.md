Release process
===============

This is a quick guide through the steps of creating an OVA and an AMI for a specific MLDB release.
For detailed instructions on how to setup the build environment and on what tunables are available, see below.


VirtualBox image (OVA)
----------------------

The build are usually done on `dev.mldb.ai` since VirtualBox is already installed there.

### VirtualBox Setup ###

First, connect to dev with X forwarding and make sure VirtualBox works:

```
ssh -X dev.mldb.ai

VirtualBox
# You should see the virtualbox window. Close it.
```

If VirtualBox complains about `/dev/vboxdrv` being missing, you'll have to reinstall the kernel headers and recompile the vbox kernel modules:

```
sudo apt-get install linux-headers-generic virtualbox-ose-dkms
sudo dpkg-reconfigure virtualbox-dkms
sudo modprobe vboxdrv
```

### OVA Build ###

This step requires packer, see [below](#installing-packer) for installation instructions.

It also requires the `vbox-MLDB-base.ova` image which can be downloaded from S3 if this is your first build:

```
# From the root of the mldbpro repo
IMG="container_provis/packer_output/vbox-mldb-base/vbox-MLDB-base.ova"
mkdir -p $(dirname "$IMG")
[ ! -f "$IMG" ] && wget https://s3.amazonaws.com/public-mldb-ai/vbox-MLDB-base.ova -O "$IMG"
```

To build the OVA:

```
# From the container_provis/ directory in the mldbpro repo:

PATH_TO_PACKER/packer build \
       -only=vbox \
       -var-file=packer-vars.json \
       -var 'mldb_docker_tag=vYYYY.MM.DD.0' \
       packer.json
```

Once it is built (5~10 minutes), upload it to S3:

```
s3cmd put --acl-public container_provis/packer_output/mldb/MLDB-20160712-latest/MLDB-20160712.ova s3://public-mldb-ai/

```

At this point, the MLDB team can QA the image.

Once the QA is complete, upload the image as `mldb.ova` on public-mldb-ai:

```
s3cmd put --acl-public container_provis/packer_output/mldb/MLDB-20160712-latest/MLDB-20160712.ova s3://public-mldb-ai/mldb.ova
```

Amazon Machine Image (AMI)
--------------------------

Build the AMI:

```
PATH_TO_PACKER/packer build \
    -only=aws \
    -var 'aws_access_key=AWS_ACCESS_KEY_ID_HERE' \
    -var 'aws_secret_key=AWS_SECRET_ACCESS_KEY_HERE' \
    -var 'mldb_docker_tag=vYYYY.MM.DD.0' \
    -var-file=container_provis/packer-vars.json \
    container_provis/packer.json
```

Then we need to distribute the ami the every amazon regions.
See below for `distami` installation instructions.

```
# This step takes a long time...
# The AMI_ID is provided in the output of the packer command launched above.
distami -p -v --region us-east-1 AMI_ID
```


Requirements
============

- [virtualbox](https://www.virtualbox.org)
- [packer](https://www.packer.io)
- [distami](https://github.com/Answers4AWS/distami)
- s3cmd (configured with Access Key ID and Secret Access Key)


Installing packer
-----------------

Official installation guidelines can be found on packer's [website](https://www.packer.io/docs/installation.html)

Quick installation notes:

```
wget https://releases.hashicorp.com/packer/0.10.1/packer_0.10.1_linux_amd64.zip
unzip packer_0.10.1_linux_amd64.zip -d packer
```

Installing distami
------------------

This is installed using pip, usually in a virtualenv:

```
virtualenv env
source env/bin/activate
pip install distami
```


Building the base ova
=====================

The MLDB ova is built in 2 steps:

 1. First, a vanilla Ubuntu 14.04 64 bits must be built.
    This step is usually not required unless you want to modify the image.
    A pre-built version of the image can be found at https://s3.amazonaws.com/public-mldb-ai/vbox-MLDB-base.ova

 2. Then a set of packages, scripts and the MLDB docker are installed on top of the first image.

To build the 'base' image, run the following command:

```
packer build  -var-file=packer-vars.json vbox-mldb-base.json
```

This will download the ubuntu 14.04.3 iso and do a basic installation in a VM.
Be patient, it takes about 10 minutes to run.

The packer template is set to do a `headless` build,
meaning that it can be built without a X server to show the vm's console.
However, this means that errors will be hard to diagnose since we're
mostly 'blind' during the install.

If you turn off headless mode and get 'Console is not powered up' errors,
you need to enable X redirection in your ssh session (`ssh -X ...`) 

Custom docker tag and image name
================================

It is possible change the docker image source and the tag during the packer invocation.

To do so, tweak these variables: `mldb_docker_image` and `mldb_docker_tag`.

For example:

```
packer build -var 'mldb_docker_tag=username_latest' [...]
```

Specifying the base image to use
================================

The path to the base image can be passed to packer by defining the `base_ovf`:

```
packer build -var 'base_ovf=/path/to/file.ovf' [...]
```

Building both the AMI and vbox image
====================================

```
packer/packer build \
    -var 'aws_access_key=AWS_ACCESS_KEY_ID_HERE' \
    -var 'aws_secret_key=AWS_SECRET_ACCESS_KEY_HERE' \
    -var-file=packer-vars.json \
    packer.json
```

Building an OVA
===============

```
packer/packer build \
    -only=vbox \
    -var-file=packer-vars.json \
    packer.json
```


Building an AMI
===============

```
packer/packer build \
    -only=aws \
    -var 'aws_access_key=AWS_ACCESS_KEY_ID_HERE' \
    -var 'aws_secret_key=AWS_SECRET_ACCESS_KEY_HERE' \
    -vars-file=packer-vars.json \
    packer.json

```

Publishing the images
=====================

```
s3cmd put packer_output/vbox-mldb-base/vbox-MLDB-base.ova s3://public-mldb-ai/

```
