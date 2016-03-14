#!/bin/bash

exec 2>&1  # stderr to stdout for logging purposes

cd {{IPYTHON_NB_DIR}}

/sbin/setuser _mldb mkdir -p {{IPYTHON_NB_DIR}}/_demos/v{{VERSION_NAME2}}
/sbin/setuser _mldb cp -pnR {{IPYTHON_DIR}}/demos/* {{IPYTHON_NB_DIR}}/_demos/v{{VERSION_NAME2}}
if [ -L {{IPYTHON_NB_DIR}}/_demos/_latest ]; then
    rm {{IPYTHON_NB_DIR}}/_demos/_latest
fi
if [ ! -f {{IPYTHON_NB_DIR}}/_demos/_latest ]; then
    ln -rs {{IPYTHON_NB_DIR}}/_demos/v{{VERSION_NAME2}} {{IPYTHON_NB_DIR}}/_demos/_latest 
fi

/sbin/setuser _mldb mkdir -p {{IPYTHON_NB_DIR}}/_tutorials/v{{VERSION_NAME2}}
/sbin/setuser _mldb cp -pnR {{IPYTHON_DIR}}/tutorials/* {{IPYTHON_NB_DIR}}/_tutorials/v{{VERSION_NAME2}}
if [ -L {{IPYTHON_NB_DIR}}/_tutorials/_latest ]; then
    rm {{IPYTHON_NB_DIR}}/_tutorials/_latest
fi 
if [ ! -f {{IPYTHON_NB_DIR}}/_tutorials/_latest ]; then
    ln -rs {{IPYTHON_NB_DIR}}/_tutorials/v{{VERSION_NAME2}} {{IPYTHON_NB_DIR}}/_tutorials/_latest 
fi

cp {{MLDB_PUBLIC_HTML_PATH}}/favicon.ico {{IPYTHON_IMAGES_DIR}}/favicon.ico
cp {{MLDB_PUBLIC_HTML_PATH}}/resources/images/mldb_ipython_logo.png {{IPYTHON_IMAGES_DIR}}/logo.png

if [ ! -e {{IPYTHON_DIR}}/profile_default ] ; then
    IPYTHONDIR={{IPYTHON_DIR}} /sbin/setuser _mldb /usr/local/bin/ipython profile create --log-level=ERROR
    /sbin/setuser _mldb mkdir -p {{IPYTHON_DIR}}/profile_default/static/custom
    /sbin/setuser _mldb cp {{IPYTHON_DIR}}/custom.js {{IPYTHON_DIR}}/profile_default/static/custom/custom.js
    /sbin/setuser _mldb cp {{IPYTHON_DIR}}/custom.css {{IPYTHON_DIR}}/profile_default/static/custom/custom.css
fi

IPYTHONDIR={{IPYTHON_DIR}}
export SHELL="/bin/bash"
exec /sbin/setuser _mldb \
        /usr/local/bin/jupyter notebook \
        --log-level=ERROR \
        --config={{IPYTHON_DIR}}/ipython_extra_config.py
 
