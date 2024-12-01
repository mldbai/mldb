// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** fixtures.cc                                 -*- C++ -*-
    Rémi Attab, 30 Apr 2013
    Copyright (c) 2013 mldb.ai inc.  All rights reserved.

    Testing fixtures.
*/


#include "mldb/utils/testing/fixtures.h"

#include "mldb/compiler/filesystem.h"
#include <iostream>
#include <unistd.h>
#include "mldb/arch/exception.h"
#include "mldb/utils/environment.h"

using namespace std;
namespace fs = std::filesystem;

namespace MLDB {

/******************************************************************************/
/* TEST FOLDER FIXTURE                                                        */
/******************************************************************************/

namespace {

const fs::path tmpDir = Environment::instance()["TMP"];
const string prefixDir = "./../../../../";

};

int TestFolderFixture::testCount = 0;

TestFolderFixture::
TestFolderFixture(const string& name) :
    name(name)
{
    path = tmpDir / (name + "_" + to_string(testCount++));

    if (fs::is_directory(path)) {
        try {
            MLDB_TRACE_EXCEPTIONS(false);
            fs::remove_all(path);
        } catch (const std::filesystem::filesystem_error & err) {
            // ignore, probably a nfs entry still hanging around
        }
    }
        
    fs::create_directories(path);

    oldPath = fs::current_path().string();
    fs::current_path(path);
}

TestFolderFixture::
~TestFolderFixture()
{
    fs::current_path(oldPath);
    try {
        MLDB_TRACE_EXCEPTIONS(false);
        fs::remove_all(path);
    } catch (const std::filesystem::filesystem_error & err) {
        // ignore, probably a nfs entry still hanging around
    }
}

string
TestFolderFixture::
resolvePath(const string& path) const
{
    return prefixDir + path;
}

string
TestFolderFixture::
uniqueName() const
{
    return name + "_" + to_string(testCount) + "_" + to_string(getuid());
}


} // namespace MLDB
