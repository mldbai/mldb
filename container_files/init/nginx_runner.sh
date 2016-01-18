#!/bin/bash

exec 2>&1  # stderr to stdout for logging purposes

exec /usr/sbin/nginx -g 'daemon off;'
