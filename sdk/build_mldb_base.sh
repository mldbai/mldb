#!/bin/sh

apt-get purge -y libcurl4-openssl-dev

apt-get install g++ make libbz2-dev liblzma-dev libcrypto++-dev libpqxx3-dev libicu-dev \
    libgoogle-perftools-dev liblapack-dev libblas-dev python-virtualenv \

# Clean up APT when done
apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

#git-core g++ scons emacs ccache make gdb time automake libtool autoconf bash-completion google-perftools valgrind gfortran flex bison pkg-config python-numpy python-numpy-dev python-matplotlib doxygen python-dev python-tk tk-dev python-virtualenv rake ipmitool mm-common gdb linux-tools ant python-setuptools
#    libcppunit-dev \
# libevent-dev \
#    libsigc++-2.0-dev \
#    libcairo2-dev libcairomm-1.0-dev
#    libfreetype6-dev libpng-dev \
#    libidn11 librtmp0 sudo bash

