license_links = {
        "Markdown": "https://github.com/waylan/Python-Markdown/blob/master/LICENSE.md",
        "SQLAlchemy": "http://www.sqlalchemy.org/download.html",
        "bokeh": "https://github.com/bokeh/bokeh/blob/master/LICENSE.txt",
        "boto": "https://github.com/boto/boto/blob/develop/LICENSE",
        "bottle": "https://github.com/bottlepy/bottle/blob/master/LICENSE",
        "dateutils": "https://pypi.python.org/pypi/dateutils/0.6.6",
        "flake8": "https://gitlab.com/pycqa/flake8/blob/master/LICENSE",
        "ggplot": "https://github.com/yhat/ggplot/blob/master/LICENSE",
        "http-status": "https://github.com/DanielOaks/http_status/blob/master/README.rst",
        "httplib2": "https://github.com/jcgregorio/httplib2/blob/master/LICENSE",
        "inflection": "https://github.com/jpvanhal/inflection/blob/master/LICENSE",
        "ipython[notebook]": "https://github.com/ipython/ipython/blob/master/COPYING.rst",
        "j2cli": "https://github.com/kolypto/j2cli/blob/master/LICENSE",
        "jinja2": "https://github.com/mitsuhiko/jinja2/blob/master/LICENSE",
        "jsonschema": "https://github.com/Julian/jsonschema/blob/master/COPYING",
        "matplotlib": "http://matplotlib.org/users/license.html",
        "mccabe": "https://github.com/flintwork/mccabe/blob/master/LICENSE",
        "numpy": "http://www.numpy.org/license.html",
        "openpyxl": "https://bitbucket.org/openpyxl/openpyxl/src/b90e25b098a65d90f0c232528b3ad078fb1c161b/LICENCE.rst?at=default",
        "pandas": "https://github.com/pydata/pandas/blob/master/LICENSE",
        "passlib": "https://code.google.com/p/passlib/source/browse/LICENSE",
        "patsy": "https://github.com/pydata/patsy/blob/master/LICENSE.txt",
        "pep8": "https://github.com/jcrocholl/pep8/blob/master/LICENSE",
        "psycopg2": "https://github.com/psycopg/psycopg2/blob/master/LICENSE",
        "pyaml": "https://github.com/mk-fg/pretty-yaml/blob/master/COPYING",
        "pycrypto": "https://github.com/dlitz/pycrypto/blob/master/COPYRIGHT",
        "pyflakes": "http://bazaar.launchpad.net/~pyflakes-dev/pyflakes/master/view/head:/LICENSE",
        "pygments": "https://bitbucket.org/birkenfeld/pygments-main/src/97aa09a974b29b03d4948cc1a4f31f6a14d795a1/LICENSE?at=default",
        "pymongo": "https://github.com/mongodb/mongo-python-driver/blob/master/LICENSE",
        "python-dateutil": "https://github.com/dateutil/dateutil/blob/master/LICENSE",
        "python-memcached": "http://www.tummy.com/software/python-memcached/",
        "pytz": "http://bazaar.launchpad.net/~stub/pytz/devel/view/head:/src/LICENSE.txt",
        "requests[security]": "https://github.com/kennethreitz/requests/blob/master/LICENSE",
        "scikit-learn": "https://github.com/scikit-learn/scikit-learn/blob/master/COPYING",
        "scipy": "http://www.scipy.org/scipylib/license.html",
        "seaborn": "https://github.com/mwaskom/seaborn/blob/master/LICENSE",
        "setuptools": "https://pypi.python.org/pypi/setuptools/14.3.1",
        "six": "https://bitbucket.org/gutworth/six/src/784c6a213c4527ea18f86a800f51bf16bc1df5bc/LICENSE?at=default",
        "theano": "https://github.com/Theano/Theano/blob/master/doc/LICENSE.txt",
        "uwsgi": "https://github.com/unbit/uwsgi/blob/master/LICENSE",
        "wsgi-intercept": "https://github.com/cdent/python3-wsgi-intercept/blob/master/COPYRIGHT",
        "wsgiref": "https://pypi.python.org/pypi/wsgiref",
        }


if __name__ == '__main__':
    ret = []
    for pkg in sorted(license_links.keys()):
        ret.append("[%s](%s)" % (pkg, license_links[pkg]))
    
    
    for item in ret:
        print "  *", item
