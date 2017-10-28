#!/usr/bin/env python

# Copyright mldb.ai 2017
#
# Script to build wheel packages from a requirements file.
# The script builds a temporary virtualenv, installs a few base modules
# and then proceeds to build wheel archives from all listed python modules.
# Once everything is built, all wheel files are installed from the wheel files

import argparse
import errno
import logging
import os
import platform
import shutil
import sys
import tempfile
import time
import virtualenv

progname =  os.path.basename(sys.argv[0])


PIP_BASE_REQUIREMENTS = ['wheel', 'pyopenssl', 'ndg-httpsclient', 'pyasn1']

SKLEARN_REQ_QUIRKS = ['numpy', 'scipy']

# linux_distribution() == ('Ubuntu', '12.04', 'precise')
distro = platform.linux_distribution()[0].lower()  # ubuntu
codename = platform.linux_distribution()[2].lower()  # precise
cpuarch = platform.machine()  # x86_64
# Keep this suffix in sync with services/saltmaster/wheelhouse.sls and env/pip.sls
_wheelhouse_suffix = "%s/%s/%s" % (distro, codename, cpuarch)
WHEELHOUSE_URL = 'https://wheelhouse.mldb.ai/public/%s' % (_wheelhouse_suffix)

RSYNC_CMD='rsync -rv {wheel_dir}/ wheelhouse.mldb.ai:/storage/wheelhouse/public/%s' % (_wheelhouse_suffix)

def cleanup_sys_path(global_site_dir):
    for p in ['%s/site-packages' %(global_site_dir),
              '%s/dist-packages' % (global_site_dir) ]:
        if p in sys.path:
            sys.path.remove(p)

def build_wheels(to_install):
    logger.info('Building wheels for %s', to_install)

    t1 = time.time()
    virtualenv.call_subprocess(['pip', 'wheel'] + pip_wheel_common_args + to_install)
    logger.debug('Took: %.3fs', time.time()-t1)

    logger.info('Installing all built packages')
    t1 = time.time()
    # --no-index to use wheeldir as sole source of pkgs.
    virtualenv.call_subprocess(['pip', 'install', '--find-links',
                               wheel_dir, '--no-index'] +
                               to_install)
    logger.debug('Took: %.3fs', time.time()-t1)



def _get_requirements(requirements):
    # import here to get latest version of pip installed above
    from pip.download import PipSession
    from pip.req import parse_requirements

    r = parse_requirements(requirements, session=PipSession())
    req_map = {}
    for req in r:
        req_map[req.name] = req

    return req_map

def prebuild_quirks(requirement_map):
    """ Do stuff in temp virtuanenv for some specific packages """

    if "scikit-learn" in requirement_map:
        for quirk in SKLEARN_REQ_QUIRKS:
            logger.info('Installing %s as a quirk for scikit-learn', quirk)
            # They don't declare the pkg as a build requirement
            # Install version of pkg specified in requirements, or latest
            if quirk in requirement_map:
                # This yield the form  numpy==version
                quirk_req = str(requirement_map[quirk].req)
            else:
                quirk_req = quirk
            # build and install a wheel in the venv for numpy
            build_wheels([quirk_req])


if __name__ == "__main__":
    global args
    global logger
    epilog = "For example: %s -w /tmp/wheeldir -r src/python_requirements.txt" % progname
    args_parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                          epilog=epilog)
    args_parser.add_argument('-l', '--loglevel',
                             choices=["debug", "info", "warning",
                                      "error", "critical"],
                             default='info',
                             help='Log level',)
    args_parser.add_argument('-W', '--wheelhouse-url',
                             default=WHEELHOUSE_URL,
                             help='Wheelhouse URL to use.',)
    args_parser.add_argument('--no-wheelhouse-url',
                             action='store_true',
                             default=False,
                             help='Disable use of external wheelhouse',)
    args_parser.add_argument('-w', '--wheel-dir',
                             default='./wheelhouse',
                             help='Wheel directory, used for input and output',)
    args_parser.add_argument('--keep-env',
                             action='store_true',
                             default=False,
                             help='Do not delete temporary virtualenv',)
    args_parser.add_argument('-r', '--requirements-file',
                             help='python requirements file listing all wheels to build',)
    args_parser.add_argument('module',
                             nargs=argparse.REMAINDER,
                             help='Python (PyPI) module to build a wheel for',)

    args = args_parser.parse_args()
    logging.basicConfig()
    logger = logging.getLogger(progname)
    loglevel = logging.getLevelName(args.loglevel.upper())
    logger.setLevel(loglevel)
    wheel_dir = "%s/%s" % (args.wheel_dir, _wheelhouse_suffix)
    try:
        os.makedirs(wheel_dir)
    except OSError as e:
        if e.errno == errno.EEXIST:
            pass
        else:
            raise

    pip_wheel_common_args = ['--find-links', wheel_dir,
                             '--wheel-dir', wheel_dir,]

    if args.no_wheelhouse_url:
        pip_wheel_common_args.extend(['--find-links', "/dev/null"])
    else:
        pip_wheel_common_args.extend(['--find-links', args.wheelhouse_url])

    if not args.requirements_file and not args.module:
        args_parser.print_help()
        logger.error('Specify requirements file (-r) or a module name to build')
        exit(1)
    
    tmpenv = tempfile.mkdtemp()
    try:
        # Create new venv and load it from within
        logger.info('Creating and activating virtualenv: %s', tmpenv)
        t1 = time.time()
        # Remove all paths from sys.path that are in global site
        # Inspired by virtualenv.py
        import site
        global_site_dir = os.path.dirname(site.__file__)
        virtualenv.create_environment(tmpenv)
        activate_this_file = "%s/bin/activate_this.py" % (tmpenv)
        execfile(activate_this_file, dict(__file__=activate_this_file))
        cleanup_sys_path(global_site_dir)

        virtualenv.call_subprocess(['pip', 'install', '-U', 'pip'])
        logger.debug('Took: %.3fs', time.time()-t1)

        logger.info('Installing pip base requirements')
        t1 = time.time()
        # pip 7.1.2 broke setuptools upgrade: https://github.com/pypa/pip/issues/3045
        try:
            virtualenv.call_subprocess(['pip', 'install', '-U', 'setuptools'])
        except:
            logger.info('\n\n\n\nExpected errors... see https://github.com/pypa/pip/issues/3045\n\n\n\n')
        virtualenv.call_subprocess(['pip', 'install', '-U'] + PIP_BASE_REQUIREMENTS)
        logger.debug('Took: %.3fs', time.time()-t1)

        if args.requirements_file:
            requirement_map = _get_requirements(args.requirements_file)
            prebuild_quirks(requirement_map)
            build_wheels(['-r', args.requirements_file])
        if args.module:
            # FIXME: we should probably run the quirks here too
            build_wheels(args.module)

    finally:
        logger.info('Contents of the temporary virtualenv:')
        virtualenv.call_subprocess(['pip', 'freeze', '-r', args.requirements_file])
            
        if args.keep_env:
            logger.info('Keeping virtualenv: %s', tmpenv)
        else:
            logger.info('Removing virtualenv: %s', tmpenv)
            shutil.rmtree(tmpenv)

    logger.info('Wheel packages are now in %s', wheel_dir)
    logger.info('Upload to wheelhouse using:\n\t%s',
                RSYNC_CMD.format(wheel_dir=wheel_dir))   
