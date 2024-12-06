# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

# AWS

set(LIBAWS_SOURCES
    xml_helpers.cc
    s3.cc
    sns.cc
    aws.cc
    sqs.cc
)

set(LIBAWS_INDIRECT_LINK
    arch
    value_description
    types
    cryptopp
)

set(LIBAWS_LINK
    credentials
    hash
    cryptopp
    tinyxml2
    http
    rest
    io_base
    utils
    ${LIBAWS_INDIRECT_LINK})

add_library(aws ${LIBAWS_SOURCES})
target_link_libraries(aws ${LIBAWS_LINK})

add_library(aws_vfs_handlers s3_handlers.cc)
target_link_libraries(aws_vfs_handlers aws vfs tinyxml2 http ${LIBAWS_INDIRECT_LINK})

# Set compile options for aws.cc specifically for gcc 4.7
set_source_files_properties(aws.cc PROPERTIES COMPILE_OPTIONS "-fpermissive;-I;${CRYPTOPP_INCLUDE_DIR}")

add_executable(s3tee s3tee.cc)
target_link_libraries(s3tee vfs aws arch aws_vfs_handlers Boost::program_options utils cryptopp)

add_executable(s3cp s3cp.cc)
target_link_libraries(s3cp vfs aws arch types aws_vfs_handlers Boost::program_options utils  cryptopp)

add_executable(s3_multipart_cmd s3_multipart_cmd.cc)
target_link_libraries(s3_multipart_cmd vfs aws arch aws_vfs_handlers Boost::program_options utils  cryptopp)

add_executable(s3cat s3cat.cc)
target_link_libraries(s3cat vfs aws arch aws_vfs_handlers Boost::program_options utils cryptopp)

add_executable(sns_send sns_send.cc)
target_link_libraries(sns_send arch http vfs aws Boost::program_options utils  cryptopp)

add_subdirectory(testing)
