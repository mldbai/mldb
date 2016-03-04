#!/bin/bash -e

if [ -n "$HTTP_BASE_URL" ] && [ -d /tmp/.onboot-templating ]; then
  echo "Boot time re-templating: setting base url to: $HTTP_BASE_URL" >&2
  . /tmp/.template-env.sh
  export HTTP_BASE_URL="$HTTP_BASE_URL"
  cd /tmp/.onboot-templating
  make -f onboot-templating.mk mldb >/dev/null
  rm -rf /tmp/.onboot-templating
fi
