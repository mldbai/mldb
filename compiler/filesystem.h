/** filesystem.h                                                   -*- C++ -*-
    Jeremy Barnes, 22 April 2018
    Copyright (c) 2018 Element AI Inc.  All rights reserved.
    This file is part of MLDB. Copyright 2018 mldb.ai inc. All rights reserved.

    Simple include to find the compiler's filesystem implementation
    and pull it into namespace std.
*/

#pragma once


#include "mldb/compiler/compiler.h"

#if __has_include(<filesystem>)
#  include <filesystem>
#elif __has_include(<experimental/filesystem>)
#  include <experimental/filesystem>

namespace std {

#  if defined(MLDB_FILESYSTEM_REMOVE_ALL_BUG) && MLDB_FILESYSTEM_REMOVE_ALL_BUG

namespace filesystem {

using namespace std::experimental::filesystem;

// will need to eventually guard this against old gcc versions... it's
// needed for 5.4.0

// https://gcc.gnu.org/bugzilla/show_bug.cgi?id=71313

inline std::uintmax_t
remove_all(const path& p, error_code& ec) noexcept
{
    using namespace std::experimental::filesystem;
    auto fs = symlink_status(p, ec);
    uintmax_t count = 0;
    if (ec.value() == 0 && fs.type() == file_type::directory)
        for (directory_iterator d(p, ec), end; ec.value() == 0 && d != end; ++d)
            count += std::filesystem::remove_all(d->path(), ec);
    if (ec.value())
        return -1;
    return remove(p, ec) ? ++count : -1;  // fs:remove() calls ec.clear()
}

inline std::uintmax_t
remove_all(const path& p)
{
    using namespace std::experimental::filesystem;
    auto fs = symlink_status(p);
    uintmax_t count = 0;
    if (fs.type() == file_type::directory)
        for (directory_iterator d(p), end; d != end; ++d)
            count += std::filesystem::remove_all(d->path());
    remove(p);
    return count + 1;
}

} // namespace filesystem

#  else

namespace filesystem = ::std::experimental::filesystem;

#  endif // filesystem remove_all bug
    
    
} // namespace std

#else
#  error No <filesystem> support found
#endif
