# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

set(EASYEXIF_SOURCE exif.cpp)
list(TRANSFORM EASYEXIF_SOURCE PREPEND "easyexif/")
add_library(easyexif STATIC ${EASYEXIF_SOURCE})
