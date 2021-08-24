# Building MLDB on OSX

With the recent addition of support for OSX as a development host of MLDB, the primary development
platform is now OSX (both x86_64 and M1).

These instructions describe how to set up a machine to host MLDB development and build and test
MLDB on such a machine.

## Requirements

We test building MLDB on:

* OSX Big Sur (11.4) on x86_64 (primary)
* OSX Big Sur (11.4) on M1 (primary)
* OSX Big Sur (11.2.3) on x86_64 (occasionally)
* <strike>OSX High Sierra (10.13.6) on x86_64 (occasionally)</strike>(Homebrew no longer can install the dependencies)

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
brew install gnu-time icu4c libmagic boost boost-python3 coreutils libssh2 lz4 openssl python@3.9 snappy v8 xz yaml-cpp
```

If your're on the Mac M1, some of the Python dependencies also require a Rust compiler to build:

```
brew install rust
```

We also need Python's `virtualenv` command:

```
pip3 install virtualenv
```

## Check out MLDB

We first get MLDB from it's Git repository.  There are two ways to do it, depending upon if your machine is
authenticated via its SSH key with GitHub.

### If you're not on GitHub or your machine isn't authenticated via its SSH key (anyone)

In this case, you can build MLDB but you can't push changes back upstream:

```
git clone https://github.com/mldbai/mldb.git
```

### If you're on GitHub and can access it via SSH (developers):

```
git clone git@github.com:mldbai/mldb.git
```

## Change into the MLDB subdirectory

```
cd mldb
```

## Pull the submodules

These are large: the data used for testing MLDB is about 100MB big, so it may take a while.
We ask `git` to do four at a time in parallel in order to speed it up a bit.

```
git submodule update --init --recursive --jobs=4
```

## Install the compilation cache (optional)

If you plan on doing lots of MLDB development, compilation times can be greatly reduced using a
compilation cache.  For OSX, we use sccache.

```
curl -fsSL https://gitlab.com/mldbai/mldb_build/-/raw/main/sccache-`uname -m`-darwin.gz | gzip -d > ./sccache && chmod +x ./sccache
declare -x COMPILER_CACHE=./sccache
```

## Install MLDB's Python dependencies

```
make dependencies
```

## Compile MLDB

```
make -j6 -k compile
```

## Run the test suite

```
make -j4 -k test || make -j4 -k test
```

