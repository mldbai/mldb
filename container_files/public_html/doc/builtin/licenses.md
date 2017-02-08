# License information

The **MLDB Community Edition** is a distribution of MLDB that can be built entirely from open-source software. The license is the [Apache License v2.0](https://github.com/mldbai/mldb/blob/master/LICENSE) and the source and build information can be found at [https://github.com/mldbai/mldb](https://github.com/mldbai/mldb).

The **MLDB Enterprise Edition** is a pre-built binary distribution of MLDB which includes the same software as the Community Edition plus the closed-source [MLDB Pro Plugin](ProPlugin.md). It is  available under two license options:

1. the free [MLDB End User License Agreement, Non-Commercial Software License version 1.0](/resources/MLDB_License.pdf). 
2. a license permitting commercial use, which can be purchased from MLDB.ai by <a href="mailto:mldb@mldb.ai" target="_blank">contacting mldb@mldb.ai</a>.

When you run the MLDB Enterprise Edition for the first time, you will be prompted for a license key to activate the software. You can create your Free Trial license key instantly by signing up to [MLDB.ai](https://mldb.ai/#license_management) and filling out the request form.

## Included open source software

To ease integration with the MLDB codebase and for easier distribution,
libraries have been included in the MLDB source code repository.

The following is a list of such libraries, along with links to their respective licenses and/or copyright notices.

  - [Boost move](https://github.com/mldbai/mldb/blob/master/utils/move.h)
  - [Boost pyExec](https://github.com/mldbai/mldb/blob/master/plugins/lang/python/python_loader.cc#L55)
  - [CityHash](https://github.com/mldbai/mldb/blob/master/ext/cityhash/COPYING)
  - [GNU Make Standard Library](https://github.com/mldbai/mldb/blob/master/ext/gmsl/gmsl)
  - [Google URL](https://github.com/mldbai/mldb/blob/master/ext/googleurl/LICENSE.txt)
  - [Highway Hash](https://github.com/mldbai/highwayhash/blob/master/LICENSE)
  - [Hoedown](https://github.com/mldbai/mldb/blob/master/ext/hoedown/LICENSE)
  - [JPeg9A](https://github.com/mldbai/JPeg9A/blob/master/README)
  - [JsonCpp](https://github.com/mldbai/mldb/blob/master/ext/jsoncpp/value.h)
  - [Judy](https://github.com/mldbai/mldb/blob/master/ext/judy/Judy.h)
  - [MNMLSTC Core](https://github.com/mldbai/mnmlstc/blob/master/License.rst)
  - [PFFFT](https://github.com/mldbai/pffft/blob/master/fftpack.h)
  - [RE2](https://github.com/mldbai/re2/blob/master/LICENSE)
  - [SIMD (SSE1+MMX or SSE2) implementation of exp](https://github.com/mldbai/mldb/blob/master/arch/sse2_exp.h)
  - [Snowball libstemmer](https://github.com/mldbai/mldb/blob/master/ext/libstemmer/LICENSE)
  - [dtoa](https://github.com/mldbai/mldb/blob/master/types/dtoa.c)
  - [easyexif](https://github.com/mldbai/easyexif/blob/master/LICENSE)
  - [edlib](https://github.com/mldbai/edlib/blob/master/LICENSE)
  - [eigen](https://github.com/mldbai/eigen/blob/master/COPYING.README)
  - [farmhash](https://github.com/mldbai/farmhash/blob/master/COPYING)
  - [giflib](https://github.com/mldbai/giflib/blob/master/COPYING)
  - [gmock](https://github.com/mldbai/gmock-1.7.0/blob/master/LICENSE)
  - [libgit2](https://github.com/mldbai/libgit2/blob/master/COPYING)
  - [libjson](https://github.com/mldbai/libbson/blob/master/COPYING)
  - [lockless](https://github.com/mldbai/mldb/blob/master/rest/testing/rest_collection_stress_test.cc#L30)
  - [lz4](https://github.com/mldbai/mldb/blob/master/ext/lz4/lz4.h)
  - [lzma](https://github.com/mldbai/mldb/blob/master/ext/lzma/lzma.h)
  - [mongo-c-driver](https://github.com/mldbai/mongo-c-driver/blob/master/COPYING)
  - [mongo-cxx-driver](https://github.com/mldbai/mongo-cxx-driver/blob/master/LICENSE)
  - [protobuf](https://github.com/mldbai/protobuf/blob/master/LICENSE)
  - [s2-geometry-library](https://github.com/mldbai/s2-geometry-library/blob/master/COPYING)
  - [spdlog](https://github.com/mldbai/mldb/blob/master/ext/spdlog/LICENSE)
  - [sqlite](https://github.com/mldbai/mldb/blob/master/ext/sqlite/sqlite3.h)
  - [svdlibc](https://github.com/mldbai/mldb/blob/master/ext/svdlibc/doc/svdlibc/license.html)
  - [svm](https://github.com/mldbai/mldb/blob/master/ext/svm/COPYRIGHT)
  - [tensorflow](https://github.com/mldbai/tensorflow/blob/master/LICENSE)
  - [tinyxml2](https://github.com/mldbai/mldb/blob/master/ext/tinyxml2/readme.txt)
  - [uap-core](https://github.com/mldbai/uap-core/blob/master/LICENSE)
  - [uap-cpp](https://github.com/mldbai/uap-cpp/blob/master/LICENSE)
  - [utf8cpp](https://github.com/mldbai/mldb/blob/master/ext/utf8cpp/source/utf8.h)
  - [v8](https://github.com/mldbai/v8-cross-build-output/blob/master/include/v8.h)
  - [xxhash](https://github.com/mldbai/mldb/blob/master/ext/xxhash/xxhash.h)
  - [zstd](https://github.com/mldbai/zstd/blob/master/LICENSE)


### Javascript libraries

The following javascript libraries are also included in the source tree:

  - [React generated code](https://raw.githubusercontent.com/mldbai/mldb/master/container_files/public_html/resources/queries/bundle.js)
  - [jQuery](https://github.com/mldbai/mldb/blob/master/container_files/public_html/resources/js/jquery-1.11.2.min.js)
  - [Bootstrap](https://github.com/mldbai/mldb/blob/master/container_files/public_html/resources/js/bootstrap.min.js)
  - [ACE - Ajax.org Cloud9 Editor](https://github.com/mldbai/mldb/blob/master/container_files/public_html/resources/js/ace.js)
  - [CoffeScript Compiler](https://github.com/mldbai/mldb/blob/master/container_files/public_html/resources/js/coffee-script.js)
  - [PrismJS](https://github.com/mldbai/mldb/blob/master/container_files/public_html/resources/js/prism.js)

### Python packages

Multiple python packages are included in the Docker distribution of MLDB.

The complete list of those unmodified packages can be found in the following files:

  - [python_requirements.txt](https://github.com/mldbai/mldb/blob/master/python_requirements.txt)
  - [python_requirements_mldb_base.txt](https://github.com/mldbai/mldb/blob/master/python_requirements_mldb_base.txt)
  - [python_constraints.txt](https://github.com/mldbai/mldb/blob/master/python_constraints.txt)
