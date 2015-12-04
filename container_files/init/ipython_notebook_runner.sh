#!/bin/bash

cd {{IPYTHON_NB_DIR}}

/sbin/setuser _mldb mkdir -p {{IPYTHON_NB_DIR}}/_demos
/sbin/setuser _mldb cp -pnR {{IPYTHON_DIR}}/demos/* {{IPYTHON_NB_DIR}}/_demos

/sbin/setuser _mldb mkdir -p {{IPYTHON_NB_DIR}}/_tutorials
/sbin/setuser _mldb cp -pnR {{IPYTHON_DIR}}/tutorials/* {{IPYTHON_NB_DIR}}/_tutorials

cp {{MLDB_STATIC_ASSETS_PATH}}/www/favicon.ico {{IPYTHON_IMAGES_DIR}}/favicon.ico
cp {{MLDB_STATIC_ASSETS_PATH}}/www/resources/images/mldb_ipython_logo.png {{IPYTHON_IMAGES_DIR}}/logo.png

if [ ! -e {{IPYTHON_DIR}}/profile_default ] ; then
    IPYTHONDIR={{IPYTHON_DIR}} /sbin/setuser _mldb /usr/local/bin/ipython profile create --log-level=ERROR
    /sbin/setuser _mldb mkdir -p {{IPYTHON_DIR}}/profile_default/static/custom
    /sbin/setuser _mldb cp {{IPYTHON_DIR}}/custom.js {{IPYTHON_DIR}}/profile_default/static/custom/custom.js
    /sbin/setuser _mldb cp {{IPYTHON_DIR}}/custom.css {{IPYTHON_DIR}}/profile_default/static/custom/custom.css
fi

IPYTHONDIR={{IPYTHON_DIR}} /sbin/setuser _mldb /usr/local/bin/ipython notebook --log-level=ERROR --config={{IPYTHON_DIR}}/ipython_extra_config.py 2>&1 | tee /var/log/ipython_notebook.log
 
