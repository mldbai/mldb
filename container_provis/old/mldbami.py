#!/usr/bin/env python
# -*- coding: utf-8 -*-

# **********
# Filename:        mldbami.py
# Description:     Transform a Ubuntu 12.04 machine into the MLDB AMI
# Author:          Marc Vieira-Cardinal
# Creation Date:   April 01, 2015
# Revision Date:   June 15, 2015
# **********


# General imports
import sys
import cuisine
import subprocess
from fabric.api import *
from fabric.colors import green, yellow, red, blue
from fabric.contrib.console import confirm
from fabric.contrib import files
from fabric.operations import prompt


@task(default=True)
def DoIt():
    print green("DoIt started")

    print yellow("Installing packages")
    cuisine.package_update()
    cuisine.package_ensure("linux-image-generic-lts-trusty")
    cuisine.package_ensure("wget")

    print yellow("Installing docker")
    sudo("wget -qO- https://get.docker.com/ | sh")

    print yellow("Adding ubuntu to the docker group")
    try:
        cuisine.group_user_ensure("docker", "ubuntu")
    except:
        pass

    print yellow("Creating mldb storage")
    sudo("install -d -o ubuntu -g ubuntu /mldb_data")

    print yellow("Setting up the mldb start on boot")
    confTmpl = cuisine.text_strip_margin(
        """
        |# On the first boot, this will pull the latest mldb Container
        |# subsequent executions will keep running the same (pulled) version
        |description "MLDB Container"
        |author "mldb.ai inc."
        |start on filesystem and started docker
        |stop on runlevel [!2345]
        |respawn
        |script
        |    /usr/bin/docker run -a stdin -a stdout -a stderr -v /mldb_data:/mldb_data -e MLDB_IDS="1000" -p 127.0.0.1:80:80 quay.io/datacratic/mldb:latest
        |end script
        |
        """):
    cuisine.file_write("/etc/init/mldb.conf",
            confTmpl, mode = "644",
            owner = "root", group = "root",
            sudo = True, check = True)

    print yellow("Configuring banner")
    confTmpl = cuisine.text_strip_margin(
        """
        |    The Virtual Appliance has booted, but you do not need to log into the
        |    Virtual Appliance to use MLDB.
        |
        |    Once MLDB is up and running inside the Virtual Appliance, you will be
        |    able to connect to it via HTTP as per the documentation.
        |
        |    Note that MLDB can take a few minutes to initialize the first time you
        |    launch the Virtual Appliance.
        | 
        | 
        """)
    cuisine.file_write("/etc/issue",
            confTmpl, mode = "644",
            owner = "root", group = "root",
            sudo = True, check = True)

    print yellow("Removing grub delay")
    sudo('echo "GRUB_TIMEOUT=0\n" >> /etc/default/grub')
    sudo("update-grub")

    print yellow("Cleaning up...")
    sudo("cat /dev/null > /home/ubuntu/.ssh/authorized_keys")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Invoke fabric if an argument was passed
        subprocess.call(['fab', '-f', __file__] + sys.argv[1:])
    else:
        # Otherwise list our targets
        subprocess.call(['fab', '-f', __file__, '--list'])
