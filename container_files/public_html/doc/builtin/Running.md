<style>
table {font-size: 18px; border: 2px solid white; border-collapse: collapse;}
td, th {padding: 12px;}
hr {margin: 40px;}
#try {
        color: #fff;
    background-color: #337ab7;
    border-color: #2e6da4;
        display: inline-block;
    padding: 6px 12px;
    margin-bottom: 0;
    font-size: 14px;
    font-weight: 400;
    line-height: 1.42857143;
    text-align: center;
    white-space: nowrap;
    text-decoration: none;
    vertical-align: middle;
    -ms-touch-action: manipulation;
    touch-action: manipulation;
    cursor: pointer;
    -webkit-user-select: none;
    -moz-user-select: none;
    -ms-user-select: none;
    user-select: none;
    background-image: none;
    border: 1px solid transparent;
    border-radius: 4px;
        padding: 10px 16px;
    font-size: 18px;
    line-height: 1.3333333;
    border-radius: 6px;
}
</style>

# Running MLDB

The easiest way to get started with MLDB is to [sign up to MLDB.ai](https://mldb.ai/#signup) and launch a free 90-minute hosted MLDB session. You'll be able to browse the live documentation and run and modify the demos yourself. You can even try running MLDB on some of your own data.

<p align="center"><a id="try" href="https://mldb.ai/#signup" target="_blank"> Try MLDB online right now! </a></p>


## Installing MLDB locally

If you want to run MLDB on your own hardware, either bare metal or in the cloud, you can choose between the open-source Community Edition and the closed-source Enterprise Edition. Building the Community Edition takes about an hour on powerful hardware but you can be up and running with the Enterprise Edition Free Trial in about 5 minutes.

          &nbsp;            | Community Edition | Enterprise Edition <br/> Free Trial | Enterprise Edition
:---------------------------:|:-:|:-:|:-:
**MLDB**    | ✓ | ✓ | ✓
**MLDB Pro Plugin[<sup>?</sup>](ProPlugin.md)**            |  &nbsp; | ✓ | ✓
**Licensing**     | [Apache License v2.0](https://github.com/mldbai/mldb/blob/master/LICENSE) | [Non-Commercial](licenses.md) | [Commercial](licenses.md)
**Pricing**     | free! |  free! | <a href="mailto:info@mldb.ai?Subject=MLDB%20pricing%20inquiry">contact sales</a>
**Support**     | [Github Issues](https://github.com/mldbai/mldb/issues/new) | <a href="mailto:info@mldb.ai?Subject=MLDB%20support">MLDB Support</a> | <a href="mailto:info@mldb.ai?Subject=MLDB%20support">MLDB Support</a>
**Getting Started**   | [build from source](https://github.com/mldbai/mldb/blob/master/Building.md)<p>takes about 1 hr |  [download](#packages) <p>takes about 5 min |  [download](#packages) <p>takes about 5 min

### Creating a Free Trial license key is easy

When you run the MLDB Enterprise Edition for the first time, you will be prompted for a license key to activate the software. You can create your Free Trial license key instantly by signing up to [MLDB.ai](https://mldb.ai/#license_management) and filling out the request form.

------------------------

<a name="packages"></a>
## Downloading and Running the MLDB Enterprise Edition

The MLDB Enterprise Edition is available as a pre-built binary in the following formats:

1. a Docker image, runnable wherever [Docker][docker] will run (recommended for **Linux, Private Cloud**)
    * see [Getting Started with the Docker image](#docker)
1. a Virtual Appliance (OVA), runnable wherever [VirtualBox][vb] will run (recommended for **OSX, Windows**)
    * see [Getting Started with the Virtual Appliance (OVA)](#virtualbox)
1. an Amazon Machine Image (AMI), runnable on the [Amazon Elastic Compute Cloud][ec2] (recommended for **AWS EC2**)
    * see [Getting Started with the Amazon Machine Image (AMI)](#aws)

[docker]: https://www.docker.com/
[ec2]: http://aws.amazon.com/ec2
[vb]: https://www.virtualbox.org/

------------------------

<a name="docker"></a>
![Docker](img/logo_docker.png)

## Getting Started with the Docker image

The Docker image is the basic MLDB distribution, and the other distribution mechanisms are just prepackaged virtual machines which run the Docker image as a container on boot.

This is the recommended way to use MLDB on Linux or Private Clouds, as well as in production.

**Step 0 - Install Docker**
    
You can get MLDB as a Docker image and run it as a Docker container. If you've never heard of or installed Docker, check out these [Docker](https://docs.docker.com/installation/) installation instructions</a> for various platforms.

*Note: We recommend using the latest stable version of Docker. Docker versions below 1.0 are not supported.*

**Step 1 - Launch an MLBD container with a mapped directory**
    
*Note: the following procedure is meant to be run as a regular user, running the MLDB container as `root` is not recommended. See the official Docker [documentation](https://docs.docker.com/engine/installation/linux/ubuntulinux/#/create-a-docker-group) for more information regarding running containers from regular user accounts.*

First, create an *empty* directory on the host machine by running the following command, where `</absolute/path/to/mldb_data>` needs to be replaced by the absolute path on your local machine where you want your MLDB working directory to be:

```bash
mkdir </absolute/path/to/mldb_data>
```

You can now execute the following command, where `<mldbport>` is a port of your choice to be used in the next section (e.g. `8080`).

```bash
docker run --rm=true \
-v </absolute/path/to/mldb_data>:/mldb_data \
-e MLDB_IDS="`id`" \
-p 127.0.0.1:<mldbport>:80 \
quay.io/mldb/mldb:latest
```

Once the container is booted, the path `/mldb_data` inside the container is mapped to `</absolute/path/to/mldb_data>` on the host machine, so MLDB will be able to access files at `</absolute/path/to/mldb_data>/file.ext` via the URL `file:///mldb_data/file.ext`. Read more about URLs [here](Url.md).

**Step 2 - Establish a tunnel (for remote servers)**

For security reasons, the instructions above will cause MLDB to only accept connections local to the host it was launched on. *If you are not running MLDB on your workstation*, you need to establish an SSH tunnel which forwards `<localport>` (e.g. `8080` again) from your workstation to `<mldbport>` on the remote host.

This command will do this in a terminal on OSX and Linux, or on Windows using Git Bash, MinGW or Cygwin:

```bash
ssh -f -o ExitOnForwardFailure=yes <user>@<remotehost> -L <localport>:127.0.0.1:<mldbport> -N
```

You can read on how to do this with Putty on Windows here: [Documentation](http://the.earth.li/~sgtatham/putty/0.64/htmldoc/Chapter4.html#config-ssh-portfwd), [Tutorial](http://alvinalexander.com/unix/edu/putty-ssh-tunnel-firefox-socks-proxy/2-configure-putty-ssh-tunnel-ssh-server.shtml).

**Step 3 - Activate MLDB**
    
When the line "MLDB Ready" appears in the console output, you can now point your browser to `http://localhost:<localport>/`. You can then follow the instructions.

### SSD cache

MLDB can use an SSD as a cache, to save downloads of remote files and allow memory
mapping of some kinds of datasets.  Normally it is best that this cache be on an
SSD.  In order to use the cache, you should mount it under `/ssd_cache` in the
container by adding the following to the docker command line:

```
-v </absolute/path/to/ssd_cache>:/ssd_cache
```

If MLDB is running in [Batch Mode] (BatchMode.md), the option `--cache-dir /ssd_cache`
should be added to the end of the command line.

Note that MLDB does not currently clean up the cache directory; this needs to be
done manually.

### Stopping, Restarting and Upgrading

When you launch MLDB with the commands above, your container will be called `mldb`, and will keep running even if you close the terminal you used to launch it. To stop MLDB, use `docker kill mldb`, and to restart it you re-run the command you used to launch the container.

To upgrade MLDB to the latest version hosted, just stop your container, run `docker pull quay.io/mldb/mldb:latest` and restart your container.

### Batch mode

See [Batch Mode] (BatchMode.md).

-----------

<a name="virtualbox"></a>
![VirtualBox](img/virtualbox_logo.png)

## Getting Started with the Virtual Appliance (OVA)

The Virtual Appliance is an Ubuntu Linux virtual machine, which on boot will download and run the Docker image documented above.

This is the recommended way to get started with MLDB on OSX and Windows, given the difficulty of running Docker directly on these platforms.

**Step 0 - Install VirtualBox**

You can get VirtualBox from the [official download page](https://www.virtualbox.org/wiki/Downloads)

**Step 1 - Download the MLDB Appliance (it's an OVA file)**

Click here: [http://public.mldb.ai/mldb.ova](http://public.mldb.ai/mldb.ova)

**Step 2 - Import the MLDB Appliance into VirtualBox**

Either double-click on the OVA file or open VirtualBox and choose *Import Appliance* from the *File* menu and point it at the OVA file. The Appliance by default is assigned 2GB of RAM, but you can change this setting here if you want it to use more or less.

**Step 3 - Run the MLDB Virtual Machine**

Double-click on the MLDB Appliance in the main VirtualBox window, which will launch the VM. When it's booted, you will see a login prompt, but **you do not need to log in**, you can just proceed to the next step. If you do want to log in, the username is `ubuntu` and the password is `mldb`. You can also SSH into the VM on port 2222 on localhost:

```
ssh -p 2222 ubuntu@localhost
```

**Step 4 - Activate MLDB**

Once it's running, you can point your browser to [`http://localhost:8080/`](http://localhost:8080/) and follow the instructions for activating MLDB. It may take few moments for the MLDB process to launch once the appliance is booted.


### Local data persistence and access

In the virtual machine, the MLDB Docker container boots up with the host directory `/mldb_data` mapped to `/mldb_data`. This means that any files in the virtual machine at `/mldb_data/file.ext` is available to MLDB as `file:///mldb_data/file.ext`. You can upload a file on your workstation at `/path/to/file.ext` to the VM via SCP on port 2222. The username you should use is `ubuntu` and the password is `mldb`:

```
scp -P2222 /path/to/file.ext ubuntu@localhost:/mldb_data/file.ext
```

### Stopping, Restarting and Upgrading

Inside the virtual machine, MLDB runs as a Docker container (see above) controlled by an `upstart` service. This means that you can stop/restart the container by SSHing into the machine and running `sudo stop mldb` and/or `sudo start mldb`.

To upgrade MLDB to the latest version, you will need to download a new OVA.

-----------

<a name="aws"></a>
![AWS](img/AWS_LOGO_RGB_300px.jpg)

## Getting Started with the Amazon Machine Image (AMI)

The AMI is an Ubuntu Linux virtual machine, which on boot will download and run the Docker image documented above. 

This is the recommended way to get started with MLDB on AWS.

**Step 0 - Create an Amazon Web Services account**

Browse to [http://aws.amazon.com/](http://aws.amazon.com/) and create and log into an AWS account if you don't already have one.

**Step 1 - Region**

In the N. Virginia zone region, Launch the [Create Instance Wizard](https://console.aws.amazon.com/ec2/v2/home?#LaunchInstanceWizard:) menu on your AWS Console.

**Step 2 - Choose an Amazon Machine Image (AMI)**

In the Community AMIs tab, search for "Datacratic MLDB" and launch the latest AMI (available in all regions)

**Step 3 - Choose an Instance Type**

For testing you can use the default (possibly free) t2.micro instance (more info on [pricing](http://aws.amazon.com/ec2/pricing/)) but take care to choose an instance with
enough RAM and CPU cores to load and process the data you'll be working with.

**Step 4 - Configure Instance Details**

For testing purposes these values can be left at their default value.

**Step 5 - Add Storage**

For testing purposes these values can be left at their default value.

**Step 6 - Tag Instance**

Name your machine.

**Step 7 - Configure Security Group**

Here you will need to allow traffic from your workstation's IP address/range to port 22 (SSH) of the machine.

**Step 8 - Launch!**

Launch your instance and wait until it becomes available. Before you can launch, the wizard will prompt you to set up a private key so you can log into the machine. You will need this key for the next step.

**Step 9 - Establish a tunnel**

For security reasons, MLDB on this AMI is set up to only accept connections from within the AWS host, so to connect from your workstation, you need to establish an SSH tunnel which forwards `<localport>` (e.g. `8080`) from your workstation to port 80 on the AWS host. The username you should use is `ubuntu` and you will need your private key from above.

This command will do this in a terminal on OSX and Linux, or on Windows using Git Bash, MinGW or Cygwin:

```bash
ssh -f -o ExitOnForwardFailure=yes ubuntu@<remotehost> -L <localport>:127.0.0.1:80 -N
```

You can read on how to do this with Putty on Windows here: [Documentation](http://the.earth.li/~sgtatham/putty/0.64/htmldoc/Chapter4.html#config-ssh-portfwd), [Tutorial](http://alvinalexander.com/unix/edu/putty-ssh-tunnel-firefox-socks-proxy/2-configure-putty-ssh-tunnel-ssh-server.shtml).

**Step 10 - Activate MLDB**

You can then point your browser to `http://localhost:<localport>/` and follow the instructions for activating MLDB. The first time you do this, it *will take a few minutes* before you can connect, as the virtual machine will download the Docker Image, and the download is around 400MB.


### Local data persistence and access

In the virtual machine, the MLDB Docker container boots up with the host directory `/mldb_data` mapped to `/mldb_data`. This means that any files in the virtual machine at `/mldb_data/file.ext` is available to MLDB as `file:///mldb_data/file.ext`. You can upload a file on your workstation at `/path/to/file.ext` to the VM via SCP. The username you should use is `ubuntu` and you will need your private key from above:

```
scp /path/to/file.ext ubuntu@<remotehost>:/mldb_data/file.ext
```


### Stopping, Restarting and Upgrading

Inside the virtual machine, MLDB runs as a Docker container (see above) controlled by an `upstart` service. This means that you can stop/restart the container by SSHing into the machine and running `sudo stop mldb` and/or `sudo start mldb`.

To upgrade MLDB to the latest version, you will need to launch a new AMI.




