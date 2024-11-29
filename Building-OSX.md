# Building MLDB on OSX

With the recent addition of support for OSX as a development host of MLDB, the primary development
platform is now OSX (both x86_64 and M1).

These instructions describe how to set up a machine to host MLDB development and build and test
MLDB on such a machine.

## Requirements

We test building MLDB on:

* OSX Big Sur (14.6.1) on M3 (primary)

We use Homebrew, an OSX package manager, to obtain the dependencies.

## Install Homebrew

See the official Homebrew installation instructions here: https://docs.brew.sh/Installation

```
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

This will ask you for your password, install the Xcode tools from Apple (basically, a developer toolchain),
and set up Homebrew for the current user.

## Install MLDB's dependencies

We use Homebrew to install the libraries on which MLDB depends.  MLDB comes packaged with some libraries
(those for which we need to build specially or a specific version is required); the rest of them are either
automatically available from the system, or installed here.  (We aim to make the list smaller over time and
in particular remove the dependency on `boost` eventually).

```
brew install gnu-time icu4c libmagic boost boost-python3 coreutils libssh2 lz4 openssl python@3.9 snappy v8 xz yaml-cpp libb2 sccache
```

Some of the Python dependencies also require a Rust compiler to build:

```
brew install rust
```

We also need Python's `virtualenv` command, and a couple of other system-wide Python packages
that can't easily be installed into a virtualenv for various reasons:

```
pip3 install virtualenv python-dateutil bottle requests
```

## Check out MLDB

We first get MLDB from its Git repository.  There are two ways to do it, depending upon if your machine is
authenticated via its SSH key with GitHub (if you're not sure, choose the first).

### 1: If you're not on GitHub or your machine isn't authenticated via its SSH key (anyone)

In this case, you can build MLDB but you can't push changes back upstream.

```
git clone https://github.com/mldbai/mldb.git
```

### 2: If you're on GitHub and can access it via SSH (developers):

In this case, you have access to the full set of GitHub functionality and can push code
back to GitHub.

```
git clone git@github.com:mldbai/mldb.git
```

## Change into the MLDB subdirectory

The rest of the build instructions require you to have done this step and it's easy to
forget, so don't!

```
cd mldb
```

## Pull the submodules

These are large: the data used for testing MLDB is about 100MB big, so it may take a while.
We ask `git` to do four submodules at a time in parallel in order to speed it up a bit.

```
git submodule update --init --recursive --jobs=4
```

## Configure the compilation cache (optional)

If you plan on doing lots of MLDB development, compilation times can be greatly reduced using a
compilation cache.  For OSX, we use sccache. This was installed via Homebrew above (`brew install sccache`).

```
declare -x COMPILER_CACHE=sccache
```

## Install MLDB's Python dependencies

These are installed inside MLDB's virtual environment, not system-wide, and so won't interfere
with the rest of the Python stuff that's going on in your system.

```
make dependencies
```

## Compile MLDB

This process is long.  You should use an argument to `-j` which is equal to the number of processors cores
in your system; be aware however that some files require lots of memory to build (more than 500MB) and
so you also need to ensure that your machine has sufficient RAM for the compilation.

```
make -j6 -k compile
```

It will take about 15 minutes on a relatively modern MacBook Pro.  You will see lots of output like this:

```
mldb/jml-build/clang.mk:13: building with clang++ version 12.0.5
mldb/jml-build/python.mk:7: PYTHON_VERSION_DETECTED=3.9
           [C++	 47kl  2.0M]		mldb/base/hash.cc
                 [   0.0s  0.0G  0.0c ]	mldb/base/hash.cc
                 [   0.0s  0.0G  0.0c ]	mldb/rest/testing/rest_request_binding_test.cc
           [C++	 74kl  3.2M]		mldb/vfs_handlers/aws/testing/sns_mock_test.cc
           [C++	 90kl  3.9M]		mldb/sql/testing/path_order_test.cc
                 [   0.0s  0.0G  0.0c ]	mldb/vfs_handlers/aws/testing/sns_mock_test.cc
           [C++	 85kl  3.7M]		mldb/sql/testing/path_benchmark.cc
                 [   0.0s  0.0G  0.0c ]	mldb/sql/testing/path_order_test.cc
           [C++	 86kl  3.7M]		mldb/sql/testing/eval_sql_test.cc
...
```

*If you get a failure at this step*, please re-run with the extra option `verbose_build=1` in order to show
the commands that were being run.  This will allow us to determine what caused the error:

```
make compile verbose_build=1
```

## Run the test suite

We run this with less jobs, as some of the tests load large data files and require a lot of time and memory
to complete.  Sometimes a few tests will fail spuriously, which is why we run it twice, either because
they took too much time (tests auto-fail after two minutes which can happen if the machine is loaded or
not much memory is available), or due to bugs that make them sporadically fail (we are addressing those
bugs, please let us know if you find one).

```
make -j4 -k test || make -j4 -k test
```

## Doing something useful with MLDB

MLDB is currently being re-architected and rebuilt; as a result it's no longer being packaged as a
stand-alone Docker container with integrated notebooks and a builtin REST API.  It's still possible
to use it for interesting things from the command line, however.

The best way to look at how to achieve something useful with MLDB is to look at the `testing` subdirectory,
which contains a large number of integration tests which test out most of the functionality.  Typically
I will write a script that I can run from the command-line.

The documentation from an earlier version of MLDB is still available at https://docs.mldb.ai/; this will
eventually be updated to point to the latest version.
