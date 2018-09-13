/* file_functions.h                                                -*- C++ -*-
   Jeremy Barnes, 30 January 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   


   ---

   Functions to deal with files.
*/

#pragma once

#include <functional>
#include <memory>
#include <string>
#include <stdint.h>

namespace MLDB {


size_t get_file_size(int fd);

size_t get_file_size(const std::string & filename);

std::string get_link_target(const std::string & link);

std::string get_name_from_fd(int fd);

typedef std::pair<uint32_t, uint32_t> inode_type;

inode_type get_inode(const std::string & filename);

inode_type get_inode(int fd);

void delete_file(const std::string & filename);

/* wrappers around fcntl with F_GETFL/F_SETFL */
void set_file_flag(int fd, int newFlag);
void unset_file_flag(int fd, int oldFlag);
bool is_file_flag_set(int fd, int flag);

void set_permissions(std::string filename,
                     const std::string & perms,
                     const std::string & group);

/** Call fdatasync on the file. */
void syncFile(const std::string & filename);

/** Does the file exist? */
bool fileExists(const std::string & filename);

} // namespace MLDB
