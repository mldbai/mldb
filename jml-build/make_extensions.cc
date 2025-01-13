// THIS FILE is released under the GPL 2.0 or later at the user's choice

#include <stdlib.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>

#include <iostream>

#include "md5.h"

using namespace std;

extern "C" {
#include <gnumake.h>
} // extern "C"


int plugin_is_GPL_compatible;

char *
md5sum(const char *nm, int argc, char **argv)
{
    MD5 md5;
    std::string result = md5(std::string(argv[0]));
    char * res = gmk_alloc(result.size() + 1);
    strncpy(res, result.c_str(), result.size() + 1);
    return res;
}

extern "C" {

int
mldb_make_extensions_init ()
{
  /* Register the function with make name "mk-temp".  */
  gmk_add_function ("md5sum", (gmk_func_ptr)md5sum, 1, 1, 1);
  return 1;
}

} // extern "C"