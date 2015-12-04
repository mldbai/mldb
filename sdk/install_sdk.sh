#!/bin/sh

apt-get -y update

apt-get -y install make g++ 

apt-get -y clean && rm -rf /var/lib/apt/lists/* /var/tmp/*

