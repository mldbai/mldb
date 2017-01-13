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

/sbin/setuser _mldb mkdir -p {{IPYTHON_NB_DIR}}/_other/v{{VERSION_NAME2}}
/sbin/setuser _mldb cp -pnR {{IPYTHON_DIR}}/other/* {{IPYTHON_NB_DIR}}/_other/v{{VERSION_NAME2}}
if [ -L {{IPYTHON_NB_DIR}}/_other/_latest ]; then
    rm {{IPYTHON_NB_DIR}}/_other/_latest
fi
if [ ! -f {{IPYTHON_NB_DIR}}/_other/_latest ]; then
    ln -rs {{IPYTHON_NB_DIR}}/_other/v{{VERSION_NAME2}} {{IPYTHON_NB_DIR}}/_other/_latest
fi

cp {{NGINX_ROOT}}/favicon.ico {{IPYTHON_IMAGES_DIR}}/favicon.ico
cp {{MLDB_PUBLIC_HTML_PATH}}/resources/images/mldb_ipython_logo.png {{IPYTHON_IMAGES_DIR}}/logo.png

export JUPYTER_CONFIG_DIR={{IPYTHON_DIR}}/config
export SHELL="/bin/bash"

/sbin/setuser _mldb \
        /usr/local/bin/jupyter nbextension enable --py --log-level=WARN widgetsnbextension

exec /sbin/setuser _mldb \
        /usr/local/bin/jupyter notebook \
        --log-level=ERROR

