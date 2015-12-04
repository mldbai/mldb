# License information

MLDB is distributed under the [MLDB End User License Agreement, Non-Commercial Software License version 1.0](/resources/MLDB_License.pdf) but the MLDB distribution includes a variety of open-source components, listed here.

# Included software

The MLDB docker container is built on 2 base layers which include numerous open-source software packages.

## Bottom layer - `phusion/baseimage-docker`

The bottom layer is a rebuild of the [phusion/baseimage-docker](https://github.com/phusion/baseimage-docker) image version `0.9.17`,
a minimal Ubuntu 14.04 base image modified for docker friendliness.
The image has been rebuilt to use the latest revisions of the Ubuntu package, including security updates.
Doing so results in smaller upper layers and total container size.

Instructions to (re)build the image can be found in the [readme](https://github.com/datacratic/baseimage-docker/blob/rel-0.9.17/README.md#building-the-image-yourself) file.

There are currently no local modification done to the base image, but if the need arises, such modifications would be documented in the [git repository](https://github.com/datacratic/baseimage-docker).

## Top layer - `mldb_base`

This layer contains software required to offer a smooth MLDB experience.
Packages such as nginx, system libraries and python packages such as Jupyter Notebook are installed in this layer.

The script used to build this image can be found [here](https://github.com/datacratic/mldb_base/blob/master/docker_create_mldb_base.sh).

### Building `mldb_base`

To build the `mldb_base` layer, run the script like this:
```bash
./docker_create_mldb_base.sh -i OUTPUT_IMAGE_NAME
```

## Licences and copyright notices

For all software included as `.deb` packages, copyright notices and licenses can be found in the `/usr/share/doc/_pkgname_/copyright` file inside the container.

### Bundled libraries

To ease integration with the MLDB codebase and for easier distribution, a few libraries are bundled with MLDB.
The following is a list of such libraries, along with links to their respective licences copyright notices.

 - [cityhash](https://github.com/google/cityhash/blob/master/COPYING)
 - [googleurl](https://code.google.com/p/google-url/source/browse/trunk/LICENSE.txt)
 - [hoedown](https://github.com/hoedown/hoedown/blob/master/LICENSE)
 - [SQLite](https://www.sqlite.org/copyright.html)
 - [TinyXML-2](https://github.com/leethomason/tinyxml2#license)

### Python packages

The following is a list of python packages, along with a link to their respective license, that were  installed in the container as requirement for MLDB.

All of these packages, and their dependencies, are installed with `pip install ...` in the `mldb_base` layer. They are all unmodified.

  * [Markdown](https://github.com/waylan/Python-Markdown/blob/master/LICENSE.md)
  * [bokeh](https://github.com/bokeh/bokeh/blob/master/LICENSE.txt)
  * [bottle](https://github.com/bottlepy/bottle/blob/master/LICENSE)
  * [dateutils](https://pypi.python.org/pypi/dateutils/0.6.6)
  * [flake8](https://gitlab.com/pycqa/flake8/blob/master/LICENSE)
  * [ggplot](https://github.com/yhat/ggplot/blob/master/LICENSE)
  * [ipython[notebook]](https://github.com/ipython/ipython/blob/master/COPYING.rst)
  * [j2cli](https://github.com/kolypto/j2cli/blob/master/LICENSE)
  * [Jinja2](https://github.com/mitsuhiko/jinja2/blob/master/LICENSE)
  * [jsonschema](https://github.com/Julian/jsonschema/blob/master/COPYING)
  * [jupyter](https://github.com/jupyter/jupyter_core/blob/master/COPYING.md)
  * [matplotlib](http://matplotlib.org/users/license.html)
  * [mccabe](https://github.com/flintwork/mccabe/blob/master/LICENSE)
  * [numpy](http://www.numpy.org/license.html)
  * [openpyxl](https://bitbucket.org/openpyxl/openpyxl/src/b90e25b098a65d90f0c232528b3ad078fb1c161b/LICENCE.rst?at=default)
  * [pandas](https://github.com/pydata/pandas/blob/master/LICENSE)
  * [patsy](https://github.com/pydata/patsy/blob/master/LICENSE.txt)
  * [pep8](https://github.com/jcrocholl/pep8/blob/master/LICENSE)
  * [pyaml](https://github.com/mk-fg/pretty-yaml/blob/master/COPYING)
  * [pycrypto](https://github.com/dlitz/pycrypto/blob/master/COPYRIGHT)
  * [pyflakes](http://bazaar.launchpad.net/~pyflakes-dev/pyflakes/master/view/head:/LICENSE)
  * [Pygments](https://bitbucket.org/birkenfeld/pygments-main/src/97aa09a974b29b03d4948cc1a4f31f6a14d795a1/LICENSE?at=default)
  * [python-dateutil](https://github.com/dateutil/dateutil/blob/master/LICENSE)
  * [python-prctl](https://raw.githubusercontent.com/seveas/python-prctl/master/COPYING)
  * [pytz](http://bazaar.launchpad.net/~stub/pytz/devel/view/head:/src/LICENSE.txt)
  * [requests[security]](https://github.com/kennethreitz/requests/blob/master/LICENSE)
  * [scikit-learn](https://github.com/scikit-learn/scikit-learn/blob/master/COPYING)
  * [scipy](http://www.scipy.org/scipylib/license.html)
  * [seaborn](https://github.com/mwaskom/seaborn/blob/master/LICENSE)
  * [setuptools](https://pypi.python.org/pypi/setuptools/14.3.1)
  * [six](https://bitbucket.org/gutworth/six/src/784c6a213c4527ea18f86a800f51bf16bc1df5bc/LICENSE?at=default)
  * [theano](https://github.com/Theano/Theano/blob/master/doc/LICENSE.txt)
  * [twine](https://raw.githubusercontent.com/pypa/twine/master/LICENSE)
  * [wheel](https://bitbucket.org/pypa/wheel/src/1cb7374c9ea4d5c82992f1d9b24cf7168ca87707/LICENSE.txt?at=default&fileviewer=file-view-default)
  * [uWSGI](https://github.com/unbit/uwsgi/blob/master/LICENSE)

